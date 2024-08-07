import DBWriter

def stationInstalled():
    # Subtract sensors installed in one station from inventory database
    dataset = "`stomato-info.technician_portal.inventory`"
    dml_statement = (
            "Update " + dataset + "set loggers = loggers - 1, atmos14 = atmos14 - 1, terros10 = terros10 - 3, "
                                  "IR = IR - 1, switch = switch - 1"
    )

    DBWriter.dbwriter.run_dml(dml_statement)


def calculateStationsAvailable():
    # Grabs # of sensors from inventory database
    dataset = "`stomato-info.technician_portal.inventory`"
    dml_statement = (
            "Select sum(loggers) as loggers_available, sum(atmos14) as atmos14_available, "
            "sum(terros10) as terros10_available, sum(IR) as IR_available,"
            "sum(switch) as switch_available from " + dataset + " where true"
    )
    # print(dml_statement)
    print("Grabbing # of sensors from inventory Database")
    result = DBWriter.dbwriter.run_dml(dml_statement)

    # Assigns the results from database query to variables
    try:
        for row in result:
            loggersAvailable = row.loggers_available
            atmos14Available = row.atmos14_available
            terros10Available = row.terros10_available
            IRAvailable = row.IR_available
            switchAvailable = row.switch_available
        stationsAvailable = 0

        # As long as there are sensors available to make a complete station keep adding to stationsAvailable
        while loggersAvailable > 0 and atmos14Available > 0 and terros10Available > 0 and IRAvailable > 0 and \
                switchAvailable > 0:
            loggersAvailable = loggersAvailable - 1
            atmos14Available = atmos14Available - 1
            terros10Available = terros10Available - 3
            IRAvailable = IRAvailable - 1
            switchAvailable = switchAvailable - 1
            stationsAvailable = stationsAvailable + 1

        # Update inventory database of Stations Available
        dml_statement = (
                "Update " + dataset + "set stationsAvailable = " + str(stationsAvailable) + " where true"
        )
        # print(dml_statement)
        # Updating inventory database with number of stations available
        DBWriter.dbwriter.run_dml(dml_statement)

    # Exception to catch when inventory retrieval query fails
    except UnboundLocalError:
        print("Big Query was not successful in returning inventory results")


def updateTotalInventory(total_loggers, total_atmos14, total_terros10, total_IR, total_switch):
    # Updates total sensor inventory from arguments
    dataset = "`stomato-info.technician_portal.inventory`"
    dml_statement = (
        "update " + dataset + "set total_loggers = " + str(total_loggers) + ", total_atmos14 = " + str(total_atmos14) +
        ", total_terros10 = " + str(total_terros10) + ", total_IR = " + str(total_IR) +
        ", total_switch = " + str(total_switch) +
        " where true"
    )
    # print(dml_statement)
    DBWriter.dbwriter.run_dml(dml_statement)


# 3 tables --> Inventory, damaged, testing, Total Inventory
# Use inventory to determine average acres each station needs and use a default average acres
# to determine how many stations we need
# calculateStationsAvailable()
# updateTotalInventory(1,1,1,1,1)