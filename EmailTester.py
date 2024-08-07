from pathlib import Path

import Decagon
from EmailProcessor import EmailProcessor

#OLD
# msg = EmailMessage()
# msg['Subject'] = 'Testing emails with python'
# msg['From'] = 'stomato.morningstar@gmail.com.com'
# msg['To'] = 'jgarrido@morningstarco.com'
# msg.set_content('WARNING, CWSI is too high!')
# s = smtplib.SMTP(host='smtp.gmail.com', port=587)
# s.starttls()
# print('Login')
# s.login('stomato.morningstar@gmail.com', 'MorningStar1')
# print('Sending')
# s.send_message(msg)
# print('Done')
# s.quit()
growers = Decagon.open_pickle()
email = EmailProcessor()
# emailPath = []
emailPath = (Path("C:\\Users\javie\PycharmProjects\Stomato\Email.txt"))
with open('Email.txt', 'w') as file:
    file.write("New Websites: ")
    for g in growers:
        for f in g.fields:
            if g.name == 'Hughes':
                file.write("\n \t" + f.name)
    file.close()
email.send_email_v3(['jsalcedo@morningstarco.com', 'jgarrido@morningstarco.com'], 'New test', 'Test', emailPath, file)

