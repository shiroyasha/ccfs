#!/bin/sh
PROJECT_DIR=$(pwd)
STATE_FILE="$PROJECT_DIR/.repo_state"

ARGS=( "$@" )

function run_build() {
    git stash create > $STATE_FILE
    docker-compose build $1
}

if [ -f "$STATE_FILE" ]; then
    if [ ${#ARGS[@]} -eq 0 ]; then
        CHANGED=$(git diff $(cat .repo_state) --name-only|grep -E '.*(\.rs$|.*Dockerfile.dev$)')
        if [ ! -z "$CHANGED" ]; then
            run_build
        fi
    else
        CHANGED=$(git diff $(cat .repo_state) --name-only|grep -E "$1\/.*(\.rs$|.*Dockerfile.dev$)")
        if [ ! -z "$CHANGED" ]; then
            run_build $1
        fi
    fi
else
    run_build
fi
