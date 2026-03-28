REGISTRY   ?= ghcr.io/echomessenger
IMAGE_NAME ?= audit
VERSION    ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
IMAGE      := $(REGISTRY)/$(IMAGE_NAME):$(VERSION)
IMAGE_LATEST := $(REGISTRY)/$(IMAGE_NAME):latest
PLATFORM  ?= linux/amd64

NAMESPACE  ?= echo
HELM_CHART := charts/audit-service
HELM_RELEASE := audit-service

.PHONY: all build test docker-build docker-push run lint helm-lint helm-deploy helm-diff clean

all: build

# ── Local ─────────────────────────────────────────────────────────────────────

build:
	./gradlew bootJar --no-daemon

test:
	./gradlew test --no-daemon

test-unit:
	./gradlew test --no-daemon --tests "com.echomessenger.audit.unit.*"

test-integration:
	./gradlew test --no-daemon --tests "com.echomessenger.audit.integration.*"

lint:
	./gradlew ktlintCheck --no-daemon 2>/dev/null || echo "ktlint not configured, skipping"

run:
	./gradlew bootRun --no-daemon --args='--spring.profiles.active=local'

clean:
	./gradlew clean --no-daemon

# ── Docker ───────────────────────────────────────────────────────────────────

docker-build:
	docker build \
	    --platform $(PLATFORM) \
		--tag $(IMAGE) \
		--tag $(IMAGE_LATEST) \
		--build-arg VERSION=$(VERSION) \
		--label "org.opencontainers.image.version=$(VERSION)" \
		--label "org.opencontainers.image.created=$(shell date -u +%Y-%m-%dT%H:%M:%SZ)" \
		.

docker-push: docker-build
	docker push $(IMAGE)
	docker push $(IMAGE_LATEST)

docker-run: docker-build
	docker run --rm \
		-p 8080:8080 \
		-p 8081:8081 \
		-e CLICKHOUSE_URL=jdbc:clickhouse://host.docker.internal:8123/audit \
		-e CLICKHOUSE_USER=default \
		-e CLICKHOUSE_PASSWORD="" \
		-e KEYCLOAK_ISSUER_URI=http://host.docker.internal:8180/realms/echo \
		-e LOG_LEVEL=debug \
		$(IMAGE)

# ── Helm ─────────────────────────────────────────────────────────────────────

helm-lint:
	helm lint $(HELM_CHART) --namespace $(NAMESPACE)

helm-diff:
	helm diff upgrade $(HELM_RELEASE) $(HELM_CHART) \
		--namespace $(NAMESPACE) \
		--values $(HELM_CHART)/values.yaml

helm-deploy:
	helm upgrade --install $(HELM_RELEASE) $(HELM_CHART) \
		--namespace $(NAMESPACE) \
		--create-namespace \
		--set image.tag=$(VERSION) \
		--atomic \
		--timeout 120s \
		--wait

helm-rollback:
	helm rollback $(HELM_RELEASE) --namespace $(NAMESPACE)

# ── Utils ─────────────────────────────────────────────────────────────────────

logs:
	kubectl logs -n $(NAMESPACE) -l app.kubernetes.io/name=$(IMAGE_NAME) -f

port-forward:
	kubectl port-forward -n $(NAMESPACE) svc/$(HELM_RELEASE) 8080:8080 8081:8081
