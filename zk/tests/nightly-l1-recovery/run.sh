#!/bin/bash

run_block_checker() {
    docker compose up --exit-code-from=block-checker --build
    return $?
}

run_erigon_unwind() {
    docker compose run erigon -- state_stages_zkevm --datadir=/datadirs/hermez-mainnet --unwind-batch-no=10
    return $?
}

run_block_checker
exit_code=$?

if [ $exit_code -eq 0 ]; then
    echo "Initial docker compose up was successful. Running the erigon service with the new command."

    run_erigon_unwind
    exit_code=$?

    # unwind success?
    if [ $exit_code -eq 0 ]; then
        echo "Erigon state stages command was successful. Running the services again."

        # sync up again
        run_block_checker
        exit_code=$?

        if [ $exit_code -eq 0 ]; then
            echo "All operations completed successfully."
        else
            echo "Block checker failed on the second run with exit code $exit_code. Aborting."
            exit $exit_code
        fi
    else
        echo "Erigon state stages command failed with exit code $exit_code. Aborting."
        exit $exit_code
    fi
else
    echo "Initial docker compose up failed with exit code $exit_code. Aborting."
    exit $exit_code
fi
