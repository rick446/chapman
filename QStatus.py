from pymongo import MongoClient

'''Server Density Plugin to monitor DVAPI status'''

class QStatus(object):
    def __init__(self, agentConfig, checksLogger, rawConfig):
        self.agentConfig = agentConfig
        self.checksLogger = checksLogger
        self.rawConfig = rawConfig
        #self.checksLogger.info("rawConfig = %r", rawConfig)
        self.cli = MongoClient(rawConfig['QStatus']['qstatus_uri'])

    def run(self):
        output = {}
        all_titles = ['jazimba-collector: reserved',
                      'jazimba-ventilator: reserved',
                      'jazimba-collector: ready',
                      'jazimba-ventilator: ready',
                      'dv-collector: reserved',
                      'dv-collector: reserved',
                      's123-collector: ready']

        data = self.cli.chapman.chapman.http_message.group(('s.q', 's.status'), {}, {'c':0}, 'function(curr, acc) {acc.c += 1}')
        for d in data:
            output.update({(str(d['s.q']) + ': ' + str(d['s.status'])).replace('.','-'): d['c']})
        for title in all_titles:
            if title not in output.keys():
                output.update({title: 0.0})
        return output