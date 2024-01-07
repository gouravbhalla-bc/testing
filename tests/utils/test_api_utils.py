import pytest
import responses

from altonomy.ace import config
from altonomy.ace.common.api_utils import get_jwt_payload
from altonomy.ace.common.api_utils import get_status_code
from tests.test_helpers.utils import random_int
from tests.test_helpers.utils import random_string


@pytest.mark.usefixtures("db")
class TestApiUtils:
    """ApiUtils"""

    def test_get_status_code_invalid_auth(self) -> None:
        """should return 403 when invalid auth"""
        expected_error_messages = ["invalid token"]

        for message in expected_error_messages:
            assert get_status_code(message) == 403

    def test_get_status_code_internal_error(self) -> None:
        """should return 500 for all other error message for get_status_code"""
        error_message_len = random_int() % 50
        error_message = random_string(error_message_len)

        # Handle random error message is a valid error message
        while error_message in ["invalid token"]:
            error_message_len = random_int() % 50
            error_message = random_string(error_message_len)

        assert get_status_code(error_message) == 400

    @responses.activate
    def test_get_jwt_token(self) -> None:
        """should get jwt token with valid payload"""
        mock_response_payload = {
            "i": 5,
            "n": "admin",
            "a": False,
            "e": 1593423820.140756,
        }
        responses.add(
            "POST",
            f"{config.ALT_CLIENT_ENDPOINT}/auth_api/auth/verify",
            json=mock_response_payload,
            status=200,
        )
        err, response_data = get_jwt_payload("test", ["user_read"])
        assert response_data == mock_response_payload
        assert (
            responses.calls[0].request.body
            == b'{"token": "test", "scopes": ["user_read"]}'
        )

    @responses.activate
    def test_get_jwt_token_invalid_token(self) -> None:
        """handle invalid token"""
        mock_response_payload = {"detail": "invalid token"}
        responses.add(
            "POST",
            f"{config.ALT_CLIENT_ENDPOINT}/auth_api/auth/verify",
            json=mock_response_payload,
            status=401,
        )
        err, response_data = get_jwt_payload("test", ["user_read"])
        assert err == "invalid token"
