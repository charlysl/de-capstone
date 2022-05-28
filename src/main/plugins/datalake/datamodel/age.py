class Age():
    
    descriptions = {
        0: 'Preschool (-7)',
        1: 'Gen Z (7-22)',
        2: 'Millennial (23-38)',
        3: 'Gen X (40-54)',
        4: 'Boomer (55-75)',
        5: 'Senior (>75)',
        9: 'UNKNOWN'
    }

    infant = 0
    genz = 1
    millenial = 2
    genx = 3
    boomer = 4
    senior = 5
    unknown = 9
    
    def __init__(self, age):
        self.age = age
        
    def group(self):
        if not self.age:
            return Age.unknown
        elif self.age <= 7:
            return Age.infant
        elif self.age <= 22:
            return Age.genz
        elif self.age <= 38:
            return Age.millenial
        elif self.age <=54:
            return Age.genx
        elif self.age <= 75:
            return Age.boomer
        else:
            return Age.senior
        
    def description(self):
        return self.descriptions[self.group()]
        