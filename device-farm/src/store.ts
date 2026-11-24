import { ConditionalCheckFailedException } from "@aws-sdk/client-dynamodb";
import {
  DeleteCommand,
  type DynamoDBDocumentClient,
  PutCommand,
  QueryCommand,
  type QueryCommandInput,
  UpdateCommand,
} from "@aws-sdk/lib-dynamodb";
import { repoShaKey, type RunRecord } from "./config.js";

export const CLAIM_LEASE_SEC = 10 * 60;
export const RECORD_TTL_SEC = 30 * 24 * 60 * 60;

export interface RunStore {
  claimWorkflowRun(claimKey: string, owner: string, now: Date): Promise<boolean>;
  completeWorkflowClaim(claimKey: string, owner: string, now: Date): Promise<void>;
  releaseWorkflowClaim(claimKey: string, owner: string): Promise<void>;
  putRun(rec: RunRecord): Promise<void>;
  queryActiveRuns(): Promise<RunRecord[]>;
  queryActiveByBranch(branchKey: string): Promise<RunRecord[]>;
  queryByRepoSha(repo: string, sha: string): Promise<RunRecord[]>;
  markCompleted(runArn: string, result: string, now: Date): Promise<void>;
}

const isConditionalFailed = (err: unknown): boolean =>
  err instanceof ConditionalCheckFailedException ||
  (typeof err === "object" &&
    err !== null &&
    "name" in err &&
    (err as { name?: string }).name === "ConditionalCheckFailedException");

export class DynamoRunStore implements RunStore {
  constructor(
    private readonly db: DynamoDBDocumentClient,
    private readonly tableName: string,
  ) {}

  async claimWorkflowRun(claimKey: string, owner: string, now: Date): Promise<boolean> {
    const nowSec = Math.floor(now.getTime() / 1000);
    const item: RunRecord = {
      run_arn: claimKey,
      state: "CLAIMING",
      claim_owner: owner,
      lease_expires_at: nowSec + CLAIM_LEASE_SEC,
      created_at: now.toISOString(),
      expires_at: nowSec + RECORD_TTL_SEC,
    };
    try {
      await this.db.send(
        new PutCommand({
          TableName: this.tableName,
          Item: item,
          ConditionExpression:
            "attribute_not_exists(run_arn) OR (#s = :claiming AND lease_expires_at < :now)",
          ExpressionAttributeNames: { "#s": "state" },
          ExpressionAttributeValues: {
            ":claiming": "CLAIMING",
            ":now": nowSec,
          },
        }),
      );
      return true;
    } catch (err) {
      if (isConditionalFailed(err)) return false;
      throw err;
    }
  }

  async completeWorkflowClaim(claimKey: string, owner: string, now: Date): Promise<void> {
    const nowSec = Math.floor(now.getTime() / 1000);
    try {
      await this.db.send(
        new UpdateCommand({
          TableName: this.tableName,
          Key: { run_arn: claimKey },
          ConditionExpression: "#s = :claiming AND claim_owner = :owner",
          UpdateExpression: "SET #s = :claimed, expires_at = :exp REMOVE lease_expires_at",
          ExpressionAttributeNames: { "#s": "state" },
          ExpressionAttributeValues: {
            ":claiming": "CLAIMING",
            ":claimed": "CLAIMED",
            ":owner": owner,
            ":exp": nowSec + RECORD_TTL_SEC,
          },
        }),
      );
    } catch (err) {
      if (!isConditionalFailed(err)) throw err;
    }
  }

  async releaseWorkflowClaim(claimKey: string, owner: string): Promise<void> {
    try {
      await this.db.send(
        new DeleteCommand({
          TableName: this.tableName,
          Key: { run_arn: claimKey },
          ConditionExpression: "#s = :claiming AND claim_owner = :owner",
          ExpressionAttributeNames: { "#s": "state" },
          ExpressionAttributeValues: {
            ":claiming": "CLAIMING",
            ":owner": owner,
          },
        }),
      );
    } catch (err) {
      if (!isConditionalFailed(err)) throw err;
    }
  }

  async putRun(rec: RunRecord): Promise<void> {
    await this.db.send(new PutCommand({ TableName: this.tableName, Item: rec }));
  }

  async queryActiveRuns(): Promise<RunRecord[]> {
    return this.queryAll({
      TableName: this.tableName,
      IndexName: "state-index",
      KeyConditionExpression: "#s = :active",
      ExpressionAttributeNames: { "#s": "state" },
      ExpressionAttributeValues: { ":active": "ACTIVE" },
    });
  }

  async queryActiveByBranch(branchKey: string): Promise<RunRecord[]> {
    return this.queryAll({
      TableName: this.tableName,
      IndexName: "branch_key-index",
      KeyConditionExpression: "branch_key = :bk",
      FilterExpression: "#s = :active",
      ExpressionAttributeNames: { "#s": "state" },
      ExpressionAttributeValues: { ":bk": branchKey, ":active": "ACTIVE" },
    });
  }

  async queryByRepoSha(repo: string, sha: string): Promise<RunRecord[]> {
    return this.queryAll({
      TableName: this.tableName,
      IndexName: "sha-index",
      KeyConditionExpression: "repo_sha = :rs",
      ExpressionAttributeValues: { ":rs": repoShaKey(repo, sha) },
    });
  }

  async markCompleted(runArn: string, result: string, now: Date): Promise<void> {
    try {
      await this.db.send(
        new UpdateCommand({
          TableName: this.tableName,
          Key: { run_arn: runArn },
          ConditionExpression: "#s = :active",
          UpdateExpression: "SET #s = :completed, #r = :result, completed_at = :completedAt",
          ExpressionAttributeNames: { "#s": "state", "#r": "result" },
          ExpressionAttributeValues: {
            ":active": "ACTIVE",
            ":completed": "COMPLETED",
            ":result": result,
            ":completedAt": now.toISOString(),
          },
        }),
      );
    } catch (err) {
      if (!isConditionalFailed(err)) throw err;
    }
  }

  private async queryAll(input: QueryCommandInput): Promise<RunRecord[]> {
    const items: RunRecord[] = [];
    let lastKey: Record<string, unknown> | undefined;
    do {
      const out = await this.db.send(new QueryCommand({ ...input, ExclusiveStartKey: lastKey }));
      if (out.Items) items.push(...(out.Items as RunRecord[]));
      lastKey = out.LastEvaluatedKey;
    } while (lastKey);
    return items;
  }
}
