import threading

from kivy.clock import mainthread
from kivy.lang import Builder
from kivymd.app import MDApp

KV = '''
#: import threading threading
Screen:
    BoxLayout:
        MDSpinner:
            id: spinner
            size_hint: None, None
            size: dp(46), dp(46)
            pos_hint: {'center_x': .5, 'center_y': .5}
            active: check.active

        MDCheckbox:
            id: check
            size_hint: None, None
            size: dp(48), dp(48)
            pos_hint: {'center_x': .5, 'center_y': .4}
            active: True

        Button:
            text: 'Spinner On/Off'
            size_hint: None, None
            size: dp(150), dp(150)
            on_release: app.spinner_toggle()

        Button:
            text: 'Run Long Process'
            size_hint: None, None
            size: dp(150), dp(150)
            on_release: 
                app.spinner_toggle()
                app.long_process_thread()
                app.spinner_toggle()
'''


class Test(MDApp):
    def build(self):
        return Builder.load_string(KV)

    @mainthread
    def spinner_toggle(self):
        print('Spinner Toggle')
        app = self.get_running_app()
        if app.root.ids.spinner.active == False:
            app.root.ids.spinner.active = True
        else:
            app.root.ids.spinner.active = False

    def long_process(self):
        for x in range(1000000):
            print(x)
        self.spinner_toggle()

    def long_process_thread(self):
        self.spinner_toggle()
        threading.Thread(target=(self.long_process)).start()


Test().run()