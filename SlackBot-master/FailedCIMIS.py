class FailedCIMIS(object):
    # failedFields = []

    def __init__(self):
        self.failedFields = []
        # self.failure = Failure()

    def add_failed_field(self, field):
        self.failedFields.append(field)

    def display_failed_fields(self):
        print("Failed CIMIS Update:")
        for ind, i in enumerate(self.failedFields):
            print(ind)
            print(i)
#     def new_failure(self):
#         self.failure.
#
# class Failure(object):
#
