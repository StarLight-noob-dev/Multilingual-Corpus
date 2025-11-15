from src.models.results.types import SimpleResult, Ok, Err

class Test_Result_Funcionality:
    # Tests

    def create_result(self, should_be_ok: bool, message: str) -> SimpleResult[str, str]:
        if should_be_ok:
            return Ok(message)
        else:
            return Err(message)

    def test_okay(self):
        # Arrange

        print("Test: Okay")

        OKAY_MESSAGE = "I am okay!"

        # Act

        ok_val = self.create_result(True, OKAY_MESSAGE)

        # Assert

        assert isinstance(ok_val, Ok)
        assert not isinstance(ok_val, Err)
        assert ok_val.is_ok() == True
        assert ok_val.ok_value() == OKAY_MESSAGE

        print(f"* ok_val: {ok_val}")
        print(f"* ok_val.ok_value(): {ok_val.ok_value()}")

    def test_err(self):
        # Arrange

        print("Test 2: Err")
        ERR_MESSAGE = "I am not okay!"

        # Act

        err_val = self.create_result(False, ERR_MESSAGE)

        # Assert

        assert isinstance(err_val, Err)
        assert not isinstance(err_val, Ok)
        assert err_val.is_ok() == False
        assert err_val.err_value() == ERR_MESSAGE

        print(f"* err_val: {err_val}")
        print(f"* err_val.err_value(): {err_val.err_value()}")