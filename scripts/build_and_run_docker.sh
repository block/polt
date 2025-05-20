#!/bin/bash

go build ./cmd/polt
./polt stage --host "mysql:3306" --database=test --table=t1 --query="SELECT * FROM t1 WHERE id = 1" --started-by='github' --run-id='runid'
./polt archive --host "mysql:3306" --database=test --destination-path="t1_archived" --started-by='github' --run-id='a_runid' --staged_table=_t1_stage_runid