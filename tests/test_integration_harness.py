from tests.integration.conftest import _compose_env


def test_compose_env_provides_token_service_secrets():
    """DataHub v1.7+ quickstart has no default token-service secrets; empty ones crash system-update."""
    env = _compose_env()
    assert env["DATAHUB_TOKEN_SERVICE_SIGNING_KEY"]
    assert env["DATAHUB_TOKEN_SERVICE_SALT"]
