#!/usr/bin/env bats

load 'bats-support/load'
load 'bats-assert/load'

DIR=~/Downloads/ccfs-test-data/upload
DATA_DIR=./data/ccfs-test-data/upload

setup_file() {
    rm -rf $DIR
    mkdir -p $DIR
    run docker-compose build
    assert_success

    # Prepare test files/directories
    curl https://download.pytorch.org/libtorch/cpu/libtorch-macos-1.7.1.zip > $DIR/large_file.zip

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

assert_n_appearances_times() {
    required_appearances=$1
    file_id=$2
    name=$3[@]
    chunks=("${!name}")

    for chunk_id in $chunks; do
        appearances=$(curl -s "http://localhost:4000/api/chunks/file/$file_id" | \
                        grep -Eo "\"id\":\"$chunk_id\",\"file_id\":\"$file_id\"" | wc -l)
        assert [ $appearances -ge $required_appearances ]
    done;
}

@test "uploading a file >64MiB" {
    run docker-compose --no-ansi run cli tree
    assert_success
    # first two rows are from docker-compose run
    assert_line --index 2 $'/\r'
    assert_line --index 3 ''

    run docker-compose run cli upload $DATA_DIR/large_file.zip
    assert_success
    assert_output --partial 'Completed file upload'

    run docker-compose --no-ansi run cli tree
    assert_success
    assert_line --index 2 $'/\r'
    assert_line --index 3 $'└─ large_file.zip\r'

    json=$(curl -s http://localhost:4000/api/files?path=./large_file.zip)
    chunks=($(echo $json | jq -r '.file_info.File.chunks[]'))
    file_id=$(echo $json | jq -r '.file_info.File.id')

    assert_n_appearances_times 1 $file_id chunks
    sleep 21 # wait for replication to run
    assert_n_appearances_times 3 $file_id chunks
}

@test "uploading a file <=64MiB" {
    run docker-compose --no-ansi run cli tree
    assert_success
    # first two rows are from docker-compose run
    assert_line --index 2 $'/\r'
    assert_line --index 3 ''

    run docker-compose run cli upload $DATA_DIR/test_small_file.txt
    assert_success
    assert_output --partial 'Completed file upload'

    run docker-compose --no-ansi run cli tree
    assert_success
    assert_line --index 2 $'/\r'
    assert_line --index 3 $'└─ test_small_file.txt\r'

    json=$(curl -s http://localhost:4000/api/files?path=./test_small_file.txt)
    chunks=($(echo $json | jq -r '.file_info.File.chunks[]'))
    file_id=$(echo $json | jq -r '.file_info.File.id')

    assert_n_appearances_times 1 $file_id chunks
    sleep 21 # wait for replication to run
    assert_n_appearances_times 3 $file_id chunks
}

@test "uploading an empty dir" {
    run docker-compose --no-ansi run cli tree
    assert_success
    # first two rows are from docker-compose run
    assert_line --index 2 $'/\r'
    assert_line --index 3 ''

    run docker-compose run cli upload $DATA_DIR/empty_dir
    assert_success
    assert_output --partial 'Completed directory upload'

    run docker-compose --no-ansi run cli tree
    assert_success
    assert_line --index 2 $'/\r'
    assert_line --index 3 $'└─ empty_dir\r'
}

@test "uploading an dir with sub items" {
    run docker-compose --no-ansi run cli tree
    assert_success
    # first two rows are from docker-compose run
    assert_line --index 2 $'/\r'
    assert_line --index 3 ''

    run docker-compose run cli upload $DATA_DIR/dir_with_content
    assert_success
    assert_output --partial 'Completed directory upload'

    run docker-compose --no-ansi run cli tree
    assert_success
    assert_line --index 2 $'/\r'
    assert_line --index 3 $'└─ dir_with_content\r'
    assert_line --index 4 $'   ├─ subdir\r'
    assert_line --index 5 $'   │  └─ stuff\r'
    assert_line --index 6 $'   │     └─ items.txt\r'
    assert_line --index 7 $'   └─ test2.txt\r'

    file1_json=$(curl -s http://localhost:4000/api/files?path=dir_with_content/subdir/stuff/items.txt)
    chunks1=($(echo $file1_json | jq -r '.file_info.File.chunks[]'))
    file1_id=$(echo $file1_json | jq -r '.file_info.File.id')

    file2_json=$(curl -s http://localhost:4000/api/files?path=dir_with_content/test2.txt)
    chunks2=($(echo $file2_json | jq -r '.file_info.File.chunks[]'))
    file2_id=$(echo $file2_json | jq -r '.file_info.File.id')

    assert_n_appearances_times 1 $file1_id chunks1
    assert_n_appearances_times 1 $file2_id chunks2

    sleep 21 # wait for replication to run
    assert_n_appearances_times 3 $file1_id chunks1
    assert_n_appearances_times 3 $file2_id chunks2
}
