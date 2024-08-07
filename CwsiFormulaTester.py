from CwsiProcessor import CwsiProcessor

cwsi_processor = CwsiProcessor()
# tc, vpd, ta, cropType, rh
#                (self,        tc,         vpd,     ta,     cropType, rh = 0):
print(cwsi_processor.get_cwsi(60.278, 1.957587813, 71.258, 'almonds', return_negative=True))

