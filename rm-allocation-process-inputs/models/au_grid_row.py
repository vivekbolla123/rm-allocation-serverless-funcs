class AuGridRow:
    
    def __init__(self, au_column_length):
        self.b2brunid = ""
        self.b2crunid = ""
        self.type = ""
        self.flight = ""
        self.start_date = ""
        self.end_date = ""
        self.day_of_week = ""
        self.aircraft_type = ""
        self.aircraft_suffix = ""
        self.lid = ""
        self.capacity = ""
        self.status = ""
        self.au_column_length = au_column_length

        # Initialize class-specific attributes
        for i in range(1, self.au_column_length + 1):
            setattr(self, f'class_of_service_{i}', None)
            setattr(self, f'class_type_{i}', "")
            setattr(self, f'class_nest_{i}', "-1")
            setattr(self, f'class_rank_{i}', "-1")
            setattr(self, f'class_au_{i}', None)
            setattr(self, f'class_allotted_{i}', "-1")
            setattr(self, f'ap_restriction_{i}', "-1")

    def set_class_attribute(self, class_number, attribute, value):
        if 1 <= class_number <= self.au_column_length:
            attr_name = f'{attribute}_{class_number}'
            if hasattr(self, attr_name):
                setattr(self, attr_name, value)
            else:
                print(f'Attribute {attr_name} does not exist.')
        else:
            print('Set Invalid class number - ' + str(class_number))

    def get_class_attribute(self, class_number, attribute):
        if 1 <= class_number <= self.au_column_length:
            attr_name = f'{attribute}_{class_number}'
            if hasattr(self, attr_name):
                return getattr(self, attr_name)
            else:
                print(f'Attribute {attr_name} does not exist.')
        else:
            print('Get Invalid class number - ' + str(class_number))
