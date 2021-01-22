#!/usr/bin/env bats

load 'bats-assert/load'

setup_file() {
    echo "running before first" >&3
}

setup() {
    echo "running before each test" >&3
}

teardown() {
    echo "running after each test" >&3
}

teardown_file() {
    echo "running after last test" >&3
}

@test "addition using bc" {
    assert_equal $(echo 2+2 | bc) 4
}

@test "addition using dc" {
    assert_equal $(echo 2 2+p | dc) 4
}
