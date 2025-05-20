### Running tests with dbdeployer

We use [dbdeployer](https://github.com/datacharmer/dbdeployer) for running the tests against multiple MySQL versions at once. For example, with MySQL 8.0.32 installed from dbdeployer:

```bash
# deploy 8.0.32
dbdeployer deploy single 8.0.32
# Running tests
MYSQL_DSN="msandbox:msandbox@tcp(127.0.0.1:8032)/test" go test -v ./...
```

### Running test with docker
Eventually we will get rid off dbdeployer and use docker-compose to run the tests against multiple MySQL versions.

#### Build and run with latest MySQL version in compose.yml
```bash
cd compose/
docker compose down --volumes && docker compose up -f compose.yml -d mysql
docker compose up --build --force-recreate buildandrun
```

#### Running tests on specific MySQL version
```bash
cd compose/
docker compose down --volumes && docker compose -f compose.yml -f 8.0.32.yml up --build mysql test --abort-on-container-exit
```