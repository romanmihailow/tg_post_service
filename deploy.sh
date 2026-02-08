#!/usr/bin/env bash
set -euo pipefail

cd /home/tg_post_service

IMAGE_REPO="${IMAGE_REPO:-romanmihailow/tg_post_service}"

IMAGE_REPO="$IMAGE_REPO" docker compose pull
IMAGE_REPO="$IMAGE_REPO" docker compose up -d --remove-orphans --force-recreate
IMAGE_REPO="$IMAGE_REPO" docker compose logs --tail=80 tg_post_service
