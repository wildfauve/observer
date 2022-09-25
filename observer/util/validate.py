import validators


def valid_uri(possible_uri):
    result = validators.url(possible_uri)
    if isinstance(result, validators.utils.ValidationFailure):
        return None
    return result