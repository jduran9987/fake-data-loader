class EventFailedValidation(Exception):
    """Exception raised for specific errors in my application.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)
