import os, sys 

# Define a function that takes in an error and error details
# The function use the sys module to extract the the information about the error, including file name and error line number
# It then formats that information into an error message string

def error_message_detail(error, error_detail:sys):
    _, _, exc_tb = error_detail.exc_info() # extract the information about the error 
    file_name = exc_tb.tb_frame.f_code.co_filename #get the file name where the error ocurred
    # formating the error msg
    error_message = f"Error Occured python script name {file_name} line number {exc_tb.tb_lineno} error message {str(error)}"
    return error_message


# create a custon exception class that inherits from built in exception class
class RatingException(Exception):

    # the class has constructor that takes in an error message and error details
    def __init__(self, error_message, error_detail:sys):
        # call the error_message_detail function to format the error message
        self.error_message = error_message_detail(error=error_message, error_detail=error_detail)

    # __str__ menthod that return the error message
    def __str__(self):
        return self.error_message