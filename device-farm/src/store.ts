import { ConditionalCheckFailedException, DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  PutCommand,
  QueryCommand,
  type QueryCommandInput,
  UpdateCommand,
} from "@aws-sdk/lib-dynamodb";
import { repoShaKey, type RunRecord } from "./config.js";

export const RECORD_TTL_SEC = 30 * 24 * 60 * 60;

export class RunStore {
  private readonly db: DynamoDBDocumentClient;

  constructor(
    region: string,
    private readonly tableName: string,
  ) {
    this.db = DynamoDBDocumentClient.from(new DynamoDBClient({ region }), {
      marshallOptions: { removeUndefinedValues: true },
    });
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

  async markCompleted(runArn: string, result: string): Promise<void> {
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
            ":completedAt": new Date().toISOString(),
          },
        }),
      );
    } catch (err) {
      if (
        !(err instanceof ConditionalCheckFailedException) &&
        (err as { name?: string })?.name !== "ConditionalCheckFailedException"
      ) {
        throw err;
      }
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
