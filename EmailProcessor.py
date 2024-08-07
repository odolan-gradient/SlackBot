import os.path
import smtplib
import ssl
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


# kdydxamcjoyqlbwg

class EmailProcessor(object):
    def __init__(self):
        None

    # def send_email(self, to, subject, message, filepath=None, filename=None):
    #     s = self.auth_login()
    #     msg = EmailMessage()
    #     msg['Subject'] = subject
    #     msg['From'] = 'stomato.morningstar@gmail.com'
    #     msg['To']  = to
    #     msg.set_content(message)
    #     if filename and filepath:
    #         attachment = open(filepath, "rb")
    #
    #         part = MIMEBase('application', 'octet-stream')
    #         part.set_payload((attachment).read())
    #         encoders.encode_base64(part)
    #         part.add_header('Content-Disposition', "attachment; filename= %s" % filename)
    #
    #         msg.attach(part)
    #
    #     print()
    #     print('Sending out email to: ' + str(to))
    #     print(' Message is: ' + str(message))
    #     text = msg.as_string()
    #     s.send_message(text)
    #     # s.send_message(msg)
    #     print('Sent')
    def auth_login(self):
        s = smtplib.SMTP(host='smtp.gmail.com', port=587)
        s.starttls()
        # Hardcoded credentials for now
        s.login('stomato.morningstar@gmail.com', 'Queretaro1012')
        return s

    # OLD DEPRECATED DUE TO GOOGLE BLOCKING OFF LESS SECURE APPS
    def send_email_v2(self, to, subject, message, filepaths=None, filenames=None):
        # Python code to send mail with attachments
        # from your Gmail account

        fromaddr = 'stomato.morningstar@gmail.com'
        toaddr = to

        # instance of MIMEMultipart
        msg = MIMEMultipart()

        # storing the senders email address
        msg['From'] = fromaddr

        # storing the receivers email address
        msg['To'] = ", ".join(toaddr)

        # storing the subject
        msg['Subject'] = subject

        # string to store the body of the mail
        body = message

        # attach the body with the msg instance
        msg.attach(MIMEText(body, 'plain'))

        if filepaths:
            # open the file to be sent
            for ind, fp in enumerate(filepaths):
                split_path = os.path.split(fp)
                filename = split_path[1]
                # filename = fp.split("/")[-1]
                attachment = open(fp, "rb")

                # instance of MIMEBase and named as p
                p = MIMEBase('application', 'octet-stream')

                # To change the payload into encoded form
                p.set_payload((attachment).read())

                # encode into base64
                encoders.encode_base64(p)

                p.add_header('Content-Disposition', "attachment; filename= %s" % filename)

                # attach the instance 'p' to instance 'msg'
                msg.attach(p)

        # authentication
        try:
            s = self.auth_login()
        except Exception as e:
            print("Error in email authentication - ")
            print("Error type: " + str(e))

        # Converts the Multipart msg into a string
        text = msg.as_string()

        # sending the mail
        s.sendmail(fromaddr, toaddr, text)

        print()
        print('Sending out email to: ' + str(to))
        print(' Message is: ' + str(message))
        print('Sent')

        # terminating the session
        s.quit()

    # NEW METHOD FOR EMAILING USING APP PASSWORDS
    def send_email_v3(self, to, subject, message, filepath=None, filename=None):
        # Python code to send mail with attachments
        # from your Gmail account

        ctx = ssl.create_default_context()
        password = "cujkqddneswjmwmc"  # Your app password goes here
        sender = "stomato.morningstar@gmail.com"  # Your e-mail address
        # receiver_string = to  # Recipient's address
        receiver_list = to
        receiver_string = ';'.join(to)

        # instance of MIMEMultipart
        msg = MIMEMultipart()

        # storing the senders email address
        msg['From'] = sender

        # storing the receivers email address
        msg['To'] = receiver_string

        # storing the subject
        msg['Subject'] = subject

        # string to store the body of the mail
        body = message

        # attach the body with the msg instance
        msg.attach(MIMEText(body, 'plain'))


        filename = filename
        # filename = fp.split("/")[-1]
        attachment = open(filepath, "rb")

        # instance of MIMEBase and named as p
        p = MIMEBase('application', 'octet-stream')

        # To change the payload into encoded form
        p.set_payload((attachment).read())

        # encode into base64
        encoders.encode_base64(p)

        p.add_header('Content-Disposition', "attachment; filename= %s" % filename)

        # attach the instance 'p' to instance 'msg'
        msg.attach(p)

        # Converts the Multipart msg into a string
        message = msg.as_string()

        print('Sending out -' + str(subject) + '- email to: ' + str(to))
        with smtplib.SMTP_SSL("smtp.gmail.com", port=465, context=ctx) as server:
            server.login(sender, password)
            server.sendmail(sender, receiver_list, message)

        print('\t...Sent')
        print()

