set shell := ["bash", "-euo", "pipefail", "-c"]
set positional-arguments

mod ctcache 'ctcache/justfile'
mod device-farm 'device-farm/justfile'
mod forgejo 'forgejo/justfile'
mod github-actions 'github-actions/justfile'

# List shared infrastructure commands.
default:
    @just --list

# Enter the pinned development environment (includes just and OpenTofu).
dev:
    nix develop ./ctcache/nix

# Connect every stack to its S3 state key.
init: ctcache::init device-farm::init forgejo::init github-actions::init
    tofu -chdir=opentofu/bootstrap init

# Plan changes to the shared state bucket.
bootstrap-plan:
    tofu -chdir=opentofu/bootstrap plan -out=bootstrap.tfplan

# Apply the previously reviewed shared-state plan.
bootstrap-apply:
    tofu -chdir=opentofu/bootstrap apply bootstrap.tfplan

# First-time setup only: use local state before the bucket exists.
bootstrap-local-init:
    mv opentofu/bootstrap/backend.tf opentofu/bootstrap/backend.tf.disabled
    tofu -chdir=opentofu/bootstrap init

# After creating the bucket: explicitly migrate local bootstrap state to S3.
bootstrap-migrate:
    mv opentofu/bootstrap/backend.tf.disabled opentofu/bootstrap/backend.tf
    tofu -chdir=opentofu/bootstrap init -migrate-state

# Format the infrastructure, Go, and Nix configurations.
fmt: ctcache::fmt device-farm::fmt forgejo::fmt
    tofu fmt -recursive opentofu

# Check configuration and synchronization scripts without changing infrastructure.
check: ctcache::check device-farm::check forgejo::check github-actions::check
    tofu fmt -check -recursive opentofu
    tofu -chdir=opentofu/bootstrap validate
