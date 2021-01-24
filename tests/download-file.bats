#!/usr/bin/env bats

load 'bats-support/load'
load 'bats-assert/load'
load 'bats-file/load'

DIR=~/Downloads/ccfs-test-data/download
DATA_DIR=./data/ccfs-test-data/download

setup_file() {
    [ -e $DIR ] && rm -rf $DIR
    mkdir -p $DIR
    run docker-compose build
    assert_success

    run docker-compose up --no-build -d --scale cli=0
    assert_success
    # wait for all chunk servers to connect
    until [ $(curl -s http://localhost:4000/api/servers|jq length) -eq 3 ]; do
        sleep 1s
    done

    # Prepare test files/directories
    curl -s https://download.pytorch.org/libtorch/cpu/libtorch-macos-1.7.0.zip > $DIR/large_file.zip

    echo "Small file content" > $DIR/test_small_file.txt

    mkdir -p $DIR/empty_dir

    mkdir -p $DIR/dir_with_content/subdir/stuff
    echo "test content" > $DIR/dir_with_content/test2.txt
    echo "another test content" > $DIR/dir_with_content/subdir/stuff/items.txt

    run docker-compose run cli upload $DATA_DIR/large_file.zip
    assert_success
    run docker-compose run cli upload $DATA_DIR/test_small_file.txt
    assert_success
    run docker-compose run cli upload $DATA_DIR/empty_dir
    assert_success
    run docker-compose run cli upload $DATA_DIR/dir_with_content
    assert_success
}

setup() {
    cleanup
}

# teardown() {

# }

teardown_file() {
    run docker-compose down
    assert_success
    rm -rf $DIR
    cleanup
}

cleanup() {
    rm -f ./large_file.zip
    rm -f ./test_small_file.txt
    rm -rf ./empty_dir
    rm -rf ./dir_with_content
    rm -rf ./stuff
}

@test "downloading a file >64MiB" {
    run docker-compose run cli download ./large_file.zip
    assert_success
    assert_output --partial 'Finished downloading `large_file.zip`'
    assert_file_exist ./large_file.zip
    assert_file_size_equals ./large_file.zip 141773095
}

@test "downloading a file <=64MiB" {
    run docker-compose run cli download test_small_file.txt
    assert_success
    assert_output --partial 'Finished downloading `test_small_file.txt`'
    assert_file_exist ./test_small_file.txt
    assert [ "$(cat ./test_small_file.txt)" == "Small file content" ]
}

@test "downloading an empty dir" {
    run docker-compose run cli download ./empty_dir
    assert_success
    assert_output --partial 'Finished downloading `empty_dir`'
    assert_dir_exist ./empty_dir
    assert [ "$(ls ./empty_dir)" == "" ]
}

@test "downloading a dir with sub items" {
    run docker-compose run cli download dir_with_content
    assert_success
    assert_output --partial 'Finished downloading `dir_with_content`'
    assert_dir_exist ./dir_with_content
    assert_dir_exist ./dir_with_content/subdir
    assert_dir_exist ./dir_with_content/subdir/stuff
    assert_file_exist ./dir_with_content/test2.txt
    assert [ "$(cat ./dir_with_content/test2.txt)" == "test content" ]
    assert_file_exist ./dir_with_content/subdir/stuff/items.txt
    assert [ "$(cat ./dir_with_content/subdir/stuff/items.txt)" == "another test content" ]
}

@test "downloading item on path" {
    run docker-compose run cli download dir_with_content/subdir/stuff
    assert_success
    assert_output --partial 'Finished downloading `stuff`'
    assert_not_exist ./dir_with_content
    assert_not_exist ./subdir
    assert_dir_exist ./stuff
    assert_not_exist ./dir_with_content/test2.txt
    assert_file_exist ./stuff/items.txt
    assert [ "$(cat ./stuff/items.txt)" == "another test content" ]
}
