class Customer:
    """
    Customer data model class
    """

    def __init__(self, customer_id, email, first_name, last_name, phone_number, created_at, acquisition_channel):
        self.customer_id = customer_id
        self.email = email
        self.first_name = first_name
        self.last_name = last_name
        self.phone_number = phone_number
        self.created_at = str(created_at)
        self.acquisition_channel = acquisition_channel

    def __str__(self):
        return f"{self.first_name} {self.last_name} ({self.email})"


def customer_to_dict(customer, ctx):
    """
    Convert Customer object to dictionary for serialization
    """
    return {
        "customer_id": customer.customer_id,
        "email": customer.email,
        "first_name": customer.first_name,
        "last_name": customer.last_name,
        "phone_number": customer.phone_number,
        "created_at": customer.created_at,
        "acquisition_channel": customer.acquisition_channel
    }


def dict_to_customer(obj, ctx):
    """
    Convert dictionary to Customer object for deserialization
    """
    if obj is None:
        return None

    return Customer(
        customer_id=obj['customer_id'],
        email=obj['email'],
        first_name=obj['first_name'],
        last_name=obj['last_name'],
        phone_number=obj['phone_number'],
        created_at=obj['created_at'],
        acquisition_channel=obj['acquisition_channel']
    )