import traceback
from org.apache.nifi.processors.script import ExecuteScript
from org.apache.nifi.processor.io import StreamCallback
from java.io import BufferedReader, InputStreamReader, OutputStreamWriter

class ConvertFiles(StreamCallback) :

    def __init__(self) :
        pass

    def process(self, inputStream, outputStream) :
        try :
            self.total = 0
            reader = InputStreamReader(inputStream,"UTF-8")
            bufferedReader = BufferedReader(reader)
            writer = OutputStreamWriter(outputStream,"UTF-8")
            line = bufferedReader.readLine()
            while line != None:
                ChangedRec = line.upper()
                writer.write(ChangedRec)
                writer.write('\n')
                a=line.split(",")
                for valu in a:
                    b=valu.strip()
                    self.total += int(b)
                line = bufferedReader.readLine()
            print("Summation of Records are %s ",self.total)
            writer.flush()
            writer.close()
            reader.close()
            bufferedReader.close()
        except :
            print "Exception in Reader:"
            print '-' * 60
            traceback.print_exc(file=sys.stdout)
            print '-' * 60
            raise
            session.transfer(flowFile, ExecuteScript.REL_FAILURE)
        finally :
            if bufferedReader is not None :
                bufferedReader.close()
            if reader is not None :
                reader.close()

flowFile = session.get()
if flowFile is not None :
    ConvertFilesData = ConvertFiles()
    session.write(flowFile, ConvertFilesData)
    flowFile = session.putAttribute(flowFile, "FileSum",str(ConvertFilesData.total))
    session.transfer(flowFile, ExecuteScript.REL_SUCCESS)