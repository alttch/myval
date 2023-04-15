VERSION=$(shell grep ^version Cargo.toml|cut -d\" -f2)

all:
	@echo "what do you want to build today?"

release: publish-crate tag

publish-create:
	cargo publish

tag:
	git tag -a v${VERSION} -m v${VERSION}
	git push origin --tags
	echo "" | gh release create v${VERSION}
