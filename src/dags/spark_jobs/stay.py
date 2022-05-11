class Stay():
    
    descriptions = {
        0: '1 day',
        1: '2-7 days',
        2: '8-30 days',
        3: '>30 days',
        4: 'not departed'
    }

    day = 0
    week = 1
    month = 2
    long = 3
    not_departed = 4
    
    def __init__(self, arrdate, depdate):
        self.arrdate = arrdate
        self.depdate = depdate
        
    def group(self):
        if not self.depdate:
            return self.not_departed
        else:
            self.stay = self.depdate - self.arrdate
            
        if self.stay <= 1:
            return self.day
        elif self.stay <= 7:
            return self.week
        elif self.stay <= 30:
            return self.month
        else:
            return self.long
        
    def description(self):
        return self.descriptions[self.group()]
        