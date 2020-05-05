class MyError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)

class MyWarning(UserWarning):
    number_warning = 0
    def __init__(self, value):
        self.value = value
        MyWarning.number_warning += 1


class ErrorTrade(MyError):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)

class WarningTrade(MyWarning):
    def __init__(self, value):
        self.value = value
        MyWarning.number_warning += 1


class ErrorFetch(MyError):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)

class WarningFetch(MyWarning):
    def __init__(self, value):
        self.value = value
        MyWarning.number_warning += 1




