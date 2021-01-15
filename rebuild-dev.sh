#!/bin/bash
PROJECT_DIR=$(pwd)
STATE_FILE="$PROJECT_DIR/.repo_state"

ARGS=( "$@" )

function get_branch() {
    git status|grep -E 'On branch .*'|sed 's/On branch //'
}

function run_build() {
    docker-compose build && {
        git stash create > $STATE_FILE
        get_branch >> $STATE_FILE
    } || echo "build failed" > $STATE_FILE
}

if [ -f "$STATE_FILE" ] && [ "$(awk 'NR==1' $STATE_FILE)" != "build failed" ]; then
    CHANGED_BRANCH=$(diff <(echo "$(awk 'NR==2' $STATE_FILE)") <(echo "$(get_branch)"))
    if [ ${#ARGS[@]} -eq 0 ]; then
        CHANGED_FILES=$(git diff $(awk 'NR==1' $STATE_FILE) --name-only|grep -E '.*(\.rs$|.*Dockerfile.dev$)')
        if [ ! -z "$CHANGED_BRANCH" ] || [ ! -z "$CHANGED_FILES" ]; then
            run_build
        fi
    fi
else
    run_build
fi
