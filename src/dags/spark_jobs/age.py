class Age():
    
    descriptions = {
        0: 'Preschool (-7)',
        1: 'Gen Z (7-22)',
        2: 'Millennial (23-38)',
        3: 'Gen X (40-54)',
        4: 'Boomer (55-75)',
        5: 'Senior (>75)',
    }

    infant = 0
    genz = 1
    millenial = 2
    genx = 3
    boomer = 4
    senior = 5
    
    def __init__(self, age):
        self.age = age
        
    def group(self):
        if self.age <= 7:
            return self.infant
        if self.age <= 22:
            return self.genz
        elif self.age <= 38:
            return self.millenial
        elif self.age <=54:
            return self.genx
        elif self.age <= 75:
            return self.boomer
        else:
            return self.senior
        
    def description(self):
        return self.descriptions[self.group()]
        