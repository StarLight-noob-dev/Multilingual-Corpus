from src.models.results.types import SimpleResult, Ok, Err

@DeprecationWarning
class Test_Result_Funcionality:

    def create_result(self, should_be_ok: bool, message: str) -> SimpleResult[str, str]:
        if should_be_ok:
            return Ok(message)
        else:
            return Err(message)

    def test_okay(self):

        OKAY_MESSAGE = "I am okay!"

        ok_val = self.create_result(True, OKAY_MESSAGE)

        assert isinstance(ok_val, Ok)
        assert not isinstance(ok_val, Err)
        assert ok_val.is_ok() == True
        assert ok_val.ok_value() == OKAY_MESSAGE

    def test_err(self):
        print("Test 2: Err")
        ERR_MESSAGE = "I am not okay!"

        err_val = self.create_result(False, ERR_MESSAGE)

        assert isinstance(err_val, Err)
        assert not isinstance(err_val, Ok)
        assert err_val.is_ok() == False
        assert err_val.err_value() == ERR_MESSAGE