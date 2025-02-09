STACK_NAME ?= rust-products
FUNCTIONS := get-products get-product put-product delete-product dynamodb-streams

ARCH := aarch64-unknown-linux-gnu

.PHONY: build deploy tests

all: build tests-unit deploy tests-integ
ci: build tests-unit

build:
	cross build --release --target $(ARCH)
	rm -rf ./build
	mkdir -p ./build
	${MAKE} ${MAKEOPTS} $(foreach function,${FUNCTIONS}, build-${function})

build-%:
	mkdir -p ./build/$*
	cp -v ./target/$(ARCH)/release/$* ./build/$*/bootstrap

deploy:
	if [ -f samconfig.toml ]; \
		then sam deploy --stack-name $(STACK_NAME); \
		else sam deploy -g --stack-name $(STACK_NAME); \
	fi

tests-unit:
	cargo test --lib --bins

tests-integ:
	RUST_BACKTRACE=1 API_URL=$$(aws cloudformation describe-stacks --stack-name $(STACK_NAME) \
		--query 'Stacks[0].Outputs[?OutputKey==`ApiUrl`].OutputValue' \
		--output text) cargo test

tests-load:
	API_URL=$$(aws cloudformation describe-stacks --stack-name $(STACK_NAME) \
		--query 'Stacks[0].Outputs[?OutputKey==`ApiUrl`].OutputValue' \
		--output text) artillery run tests/load-test.yml