#!/usr/bin/python

import logging
class NullHandler(logging.Handler):
    def emit(self, record):
        pass
log = logging.getLogger('BubbaSyncEngine')
log.setLevel(logging.ERROR)
log.addHandler(NullHandler())

import Queue
import threading
import time

from EventBus import EventBusClient

class BubbaPublisher(threading.Thread):
    
    def __init__(self,q):
        
        # store params
        self.q = q
        
        # initialize the thread
        threading.Thread.__init__(self)
        self.name = 'BubbaPublisher'
        
        # start myself
        self.start()
    
    def run(self):
        
        while True:
        
            elems  = []
            
            elems += [self.q.get(block=True)]
            
            while True:
                elem   = self.q.get(block=False)
                if not elem:
                    break
                elems += [elem]
            
            print elems
            
            time.sleep(5)
            
            print 'published!'

class BubbaSyncEngine(EventBusClient.EventBusClient):
    
    def __init__(self):
        
        # store params
        
        # local variables
        self.q          = Queue.Queue(maxsize=2)
        self.publisher  = BubbaPublisher(self.q)
        
        # log
        log.info('creating instance')
        
        # initialize parent class
        EventBusClient.EventBusClient.__init__(self,
            signal      = 'parsedAppData_OAPTemperature',
            cb          = self._handleTemperatureData,
        )
        self.name       = 'BubbaSyncEngine'
    
    def _handleTemperatureData(self,sender,signal,data):
        
        assert signal=='parsedAppData_OAPTemperature'
        try:
            self.q.put(data, block = False)
            print data
        except Queue.Full:
            print 'Full!'
        else:
            print 'Growing!'
           
    
'''
        ########################################################################
        ########################################################################
        ########################################################################
        
        # initialize parent class
        threading.Thread.__init__(self)
        self.name                 = 'DataConnector_BubbaSyncEngine'
        
        # local variables
        self.goOn                           = True
        self.spreadsheetKey                 = None
        self.worksheetName                  = None
        self.googleUsername                 = None
        self.googlePassword                 = None
        self.googleClient                   = None
        self.syncLock                       = threading.Lock()
        self.syncLock.acquire()
        self.statusLock                     = threading.Lock()
        self.status                         = {}
        self.status['usernameSet']          = 'WAIT...'
        self.status['passwordSet']          = 'WAIT...'
        self.status['status']               = 'DISCONNECTED'
        self.status['numConnectionsOK']     = 0
        self.status['numConnectionsFailed'] = 0
        self.status['lastConnected']        = None
        self.status['lastDisconnected']     = None
        self.status['numPublishedOK']       = 0
        self.status['numPublishedFail']     = 0
        self.bubbaVar                       = 5000
        self.bubbaDict                      = {}
        self.bubbaDict['type']              = 'temperature'
        self.bubbaDict['mac']               = (0,0,0,0,0,0,0,0)
        self.bubbaDict['lastvalue']         = '0'
        self.bubbaDict['lastupdated']       = '0'
        self.bubbaQueue                     = Queue(maxsize=0)

        
        # connect to dispatcher
        dispatcher.connect(
            self.indicateNewData,
            signal = 'newDataMirrored',
            weak   = False,
        )
        dispatcher.connect(
            self.getStatus,
            signal = "googlestatus",
            weak   = False,
        )
        
        # start myself
        self.start()
    
    def run(self):
        
        try:
        
            # log
            log.info('thread started')
        
            while True:
                
                # kill thread
                if not self.goOn:
                    break
                
                # deal with data, if necessary
                self._dealWithData()

                
        except Exception as err:
            output  = []
            output += ['===== crash in thread {0} ====='.format(self.name)]
            output += ['\nerror:\n']
            output += [str(err)]
            output += ['\ncall stack:\n']
            output += [traceback.format_exc()]
            output  = '\n'.join(output)
            print output # critical error
            log.critical(output)
            raise
    
    #======================== public ==========================================
    
    def getStatus(self):
        with self.statusLock:
           return copy.deepcopy(self.status)
    
    def indicateNewData(self):
        try:
            self.syncLock.release()
        except:
            pass # happens when lock already released because thread is busy
    
    def tearDown(self):
        
        # log
        log.info('tearDown() called')
        
        self.goOn = False
        try:
            self.syncLock.release()
        except:
            pass # happens when lock already released because thread is busy
    
    #======================== private =========================================
    
    def _dealWithData(self):
        
        now = time.time()
        dld = DustLinkData.DustLinkData()

        #thisData = []
        
        if self.bubbaVar:
            # obtain a copy of the mirror data
            mirrorData = dispatcher.send(
                signal        = 'getMirrorData',
                data          = None,
            )
            #print mirrorData
            if mirrorData[0][1] != []:
                if mirrorData[0][1][0]['lastupdated'] != self.bubbaDict['lastupdated']:
                    self.bubbaDict['type'] = mirrorData[0][1][0]['type']
                    self.bubbaDict['mac'] = mirrorData[0][1][0]['mac']
                    self.bubbaDict['lastvalue'] = mirrorData[0][1][0]['lastvalue']
                    self.bubbaDict['lastupdated'] = mirrorData[0][1][0]['lastupdated']
                    self.bubbaQueue.put(mirrorData[0][1])
                    print self.bubbaQueue.qsize()
                    #instead of doing all this I need to take these four values
					#type, mac, lastvalue, and lastupdated and add them as a row in a queue
					#or maybe a dictionary in a queue, or an array in a queue
					#I have to handle the case where mirrordata might have a bunch of entries
					#and still figure out which elements to put into the queue and which ones are duplicates
					#then I need a handler that will take things out of the queue and put them in a spreadsheet
                
            time.sleep(1)
            
            print mirrorData
            
            assert len(mirrorData)==1
            mirrorData = mirrorData[0][1]

        
    #======================== helpers =========================================
    
    @classmethod
    def _connectToGoogle(self,googleUsername,googlePassword):
        googleClient              = gdata.spreadsheet.service.SpreadsheetsService()
        googleClient.email        = googleUsername
        googleClient.password     = googlePassword
        googleClient.source       = 'MirrorEngine'
        googleClient.ProgrammaticLogin()
        
        return googleClient
    
    @classmethod
    def _getWorksheetId(self,googleClient,spreadsheetKey,worksheetName):
        worksheetId = None
        
        feed = googleClient.GetWorksheetsFeed(spreadsheetKey)
        
        for e in feed.entry:
            thisTitle = e.title.text
            thisId    = e.id.text.split('/')[-1]
            if thisTitle==worksheetName:
                worksheetId = thisId
                break
        
        return worksheetId
    
    @classmethod
    def _getWorksheetData(self,googleClient,spreadsheetKey,worksheetId):
        return [self._rowToDict(e) for e in googleClient.GetListFeed(spreadsheetKey,worksheetId).entry]
    
    @classmethod
    def _deleteAllRows(self,googleClient,spreadsheetKey,worksheetId,newData):
        
        # get data from Google Spreadsheet
        data = googleClient.GetListFeed(spreadsheetKey,worksheetId)
        
        # delete all rows
        for i in range(len(data.entry)):
            googleClient.DeleteRow(data.entry[i])
    
    #======================== helpers' helpers ================================
    
    @classmethod
    def _rowToDict(self,e):
        thisLine = {}
        thisLine['mac'] = e.title.text
        m = re.match('type: (\S+), min: (\S+), lastvalue: (\S+), max: (\S+), lastupdated: (\S+)', e.content.text)
        if m:
            thisLine['type']          = m.group(1)
            thisLine['min']           = m.group(2)
            thisLine['lastvalue']     = m.group(3)
            thisLine['max']           = m.group(4)
            thisLine['lastupdated']   = m.group(5)
        else:
            m = re.match('type: (\S+), lastvalue: (\S+), lastupdated: (\S+)', e.content.text)
            if m:
                thisLine['type']          = m.group(1)
                thisLine['min']           = None
                thisLine['lastvalue']     = m.group(2)
                thisLine['max']           = None
                thisLine['lastupdated']   = m.group(3)
            else:
                raise SystemError('misformed spreadsheet data')
        return thisLine

'''