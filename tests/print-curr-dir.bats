#!/usr/bin/env bats

load 'bats-support/load'
load 'bats-assert/load'

DIR=~/Downloads/ccfs-test-data/print-tree
DATA_DIR=./data/ccfs-test-data/print-tree

setup_file() {
    rm -rf $DIR
    mkdir -p $DIR
    run docker-compose build
    assert_success

    echo "Small file content" > $DIR/test_small_file.txt

    mkdir -p $DIR/empty_dir

    mkdir -p $DIR/dir_with_content/subdir/stuff
    echo "test content" > $DIR/dir_with_content/test2.txt
    echo "another test content" > $DIR/dir_with_content/subdir/stuff/items.txt
}

setup() {
    run docker-compose up --no-build -d --scale cli=0
    assert_success
    # wait for all chunk servers to connect
    until [ $(curl -s http://localhost:4000/api/servers|jq length) -eq 3 ]; do
        sleep 1s
    done
}

teardown() {
    run docker-compose down
    assert_success
}

teardown_file() {
    rm -rf $DIR
}

@test "print list for empty root" {
    run docker-compose --no-ansi run cli list
    assert_success
    # first two rows are from docker-compose run
    assert_line --index 2 $'\r'
    assert_line --index 3 ''
}

@test "print list for root containing single file" {
    run docker-compose run cli upload $DATA_DIR/test_small_file.txt
    assert_success

    run docker-compose --no-ansi run cli list
    assert_success
    assert_line --index 2 $'test_small_file.txt\r'
    assert_line --index 3 ''
}

@test "print list for root containing single dir" {
    run docker-compose run cli upload $DATA_DIR/empty_dir
    assert_success

    run docker-compose --no-ansi run cli list
    assert_success
    assert_line --index 2 $'empty_dir\r'
    assert_line --index 3 ''
}

@test "print list for root containing multiple directories" {
    run docker-compose run cli upload $DATA_DIR/empty_dir
    assert_success
    run docker-compose run cli upload $DATA_DIR/dir_with_content
    assert_success

    run docker-compose --no-ansi run cli list
    assert_success
    assert_line --index 2 $'dir_with_content\r'
    assert_line --index 3 $'empty_dir\r'
    assert_line --index 4 ''
}
