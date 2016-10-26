def fetch_all(aws_paged_func, kwargs, list_field, next_token=None):
    if next_token == '':
        return []

    our_kwargs = dict(kwargs)
    if next_token is not None:
        our_kwargs['NextToken'] = next_token

    page_data = aws_paged_func(**our_kwargs)
    next_items = fetch_all(
        aws_paged_func, kwargs, list_field,
        next_token=page_data.get('NextToken', ''))

    return page_data[list_field] + next_items
