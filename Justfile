set dotenv-load := true

aws_account_id := env_var_or_default("AWS_ACCOUNT_ID", "AWS_ACCOUNT_ID")
aws_region := env_var_or_default("AWS_REGION", "us-east-1")
registry := env_var_or_default("NOIAD_ECR_REGISTRY", aws_account_id + ".dkr.ecr." + aws_region + ".amazonaws.com")
tag := env_var_or_default("TAG", "latest")
core_image := registry + "/noiad:core-" + tag
relay_image := registry + "/noiad:relay-" + tag
db_archive := "noiad-db-" + tag + ".tar.gz"

ecr-login:
    aws ecr get-login-password --region {{aws_region}} | docker login --username AWS --password-stdin {{registry}}

build target="all":
    @if [ "{{target}}" = "core" ]; then \
        docker build --platform linux/amd64 -f Dockerfile.core -t {{core_image}} .; \
    elif [ "{{target}}" = "relay" ]; then \
        docker build --platform linux/amd64 -f Dockerfile.relay -t {{relay_image}} .; \
    elif [ "{{target}}" = "all" ]; then \
        docker build --platform linux/amd64 -f Dockerfile.core -t {{core_image}} . && \
        docker build --platform linux/amd64 -f Dockerfile.relay -t {{relay_image}} .; \
    else \
        echo "usage: just build [core|relay|all]" >&2; \
        exit 1; \
    fi

all target="all":
    @if [ "{{target}}" = "core" ]; then \
        docker buildx build --platform linux/amd64 -f Dockerfile.core -t {{core_image}} --push .; \
    elif [ "{{target}}" = "relay" ]; then \
        docker buildx build --platform linux/amd64 -f Dockerfile.relay -t {{relay_image}} --push .; \
    elif [ "{{target}}" = "all" ]; then \
        docker buildx build --platform linux/amd64 -f Dockerfile.core -t {{core_image}} --push . && \
        docker buildx build --platform linux/amd64 -f Dockerfile.relay -t {{relay_image}} --push .; \
    else \
        echo "usage: just all [core|relay|all]" >&2; \
        exit 1; \
    fi

push target="all":
    @if [ "{{target}}" = "core" ]; then \
        docker push {{core_image}}; \
    elif [ "{{target}}" = "relay" ]; then \
        docker push {{relay_image}}; \
    elif [ "{{target}}" = "all" ]; then \
        docker push {{core_image}} && docker push {{relay_image}}; \
    else \
        echo "usage: just push [core|relay|all]" >&2; \
        exit 1; \
    fi

pack:
    mkdir -p deploy/artifacts
    tar -C core/data -czf deploy/artifacts/{{db_archive}} db

pull:
    CORE_IMAGE={{core_image}} RELAY_IMAGE={{relay_image}} docker compose -f docker-compose.prod.yml pull

up:
    CORE_IMAGE={{core_image}} RELAY_IMAGE={{relay_image}} docker compose -f docker-compose.prod.yml up -d

logs:
    docker compose -f docker-compose.prod.yml logs -f core relay

down:
    docker compose -f docker-compose.prod.yml down
