# write attacker file
import datawriter

#Some config items
MAX_NUMBER_OF_RECORDS = 10
TIME_FRAME_IN_SECONDS = 5
HIT_COUNT_THRESH = 1
ipList = []
dataWriter = datawriter.DataWriter()
myResult = dataWriter.getDDoSRecords(MAX_NUMBER_OF_RECORDS,HIT_COUNT_THRESH, TIME_FRAME_IN_SECONDS)
f = open("ipHitCounts.txt", "w")
for x in myResult:
    f.write("IP: " + x[0] + " count: " + str(x[1]) + "\n")
    ipList.append(x[0])
f.flush()
f.close()

f = open("allAttackData.txt", "w")
if len(ipList) > 0:
    myResult = dataWriter.getAllInfo(ipList)
    for x in myResult:
        print(x)
        f.write(x[2])
f.flush()
f.close()







