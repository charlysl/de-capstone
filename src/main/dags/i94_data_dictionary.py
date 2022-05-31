import re
import traceback

class DictionaryData():

    def __init__(self):
        self.data = dict()

    def on_column(self, column):
        self.column = column
        self.data[self.column] = dict()

    def on_pair(self, key, value):
        self.data[self.column][key] = value

    def get_data(self):
        return self.data

    
class ListData():

    def __init__(self):
        self.data = dict()

    def on_column(self, column):
        self.column = column
        self.data[self.column] = []

    def on_pair(self, key, value):
        self.data[self.column].append([key, value])

    def get_data(self):
        return self.data
    

class DataDictionaryParser():
    """
    Description: a class to parse the i94 data dictionary
    
    Constructor:
    - filepath: str - the path to the dictionary file

    Example:
    ```
    parser = DataDictionaryParser(i94_data_dictionary_filepath)
    dict_data = parser.parse(debug=False)
    ```


    Design:

    I followed a very general design technique called Jackson Structured Programming,
    aka JSP, see https://ocw.mit.edu/courses/
    6-005-elements-of-software-construction-fall-2008/resources/mit6_005f08_lec07/)

    There are much simpler ways, like writing a shell script, I just did it this way to 
    show a systematic technique for writing structured event stream parsers, which
    is a very generally applicable, and also a very robust way of parsing any kinds 
    of files.

    Data dictionary file format as a grammar:
        
    ```
    FILE := HEADER BODY
    HEADER := *HEADER_LINE
    BODY := *SECTION
    SECTION := EMPTY_LINES SECTION_BODY
    EMPTY_LINES := *EMPTY_LINE
    SECTION_BODY := SECTION_CONTENT EMPTY_LINES
    SECTION_CONTENT := COMMENT MAYBE_DATA
    COMMENT := COMMENT_START REST_OF_COMMENT
    REST_OF_COMMENT := NOT_COMMENT_END COMMENT_END_LINE
    NOT_COMMENT_END := *NOT_COMMENT_END_LINE
    MAYBE_DATA := EMPTY_LINE | VALUE_DATA
    VALUE_DATA := VALUE DATA
    DATA := *DATA_LINE

    HEADER_LINE := '^[^/]*$'
    EMPTY_LINE := '^[;\s]*$'
    VALUE := <skip>
    COMMENT_START := '/^/\* (\w+)'
    NOT_COMMENT_END := <not COMMENT_END>
    COMMENT_END := '\*/'
    DATA_LINE := "^\s*'?([^\s']+)'?.*=\s*'?([^']*)'?.*$"
    ```    
    """
    
    header_line_regex = '^[^/]*$'
    empty_line_regex = '^[;\s]*$'
    comment_start_regex = '^/\* (\w+)'
    comment_end_regex = '\*/'
    data_line_regexp = "^\s*'?([^\s']+)'?.*=\s*'?([^']*)'?.*$"
    
    def __init__(self, filepath, output=DictionaryData()):
        self.filepath = filepath
        self.data = output
        
    def parse(self, debug=False):
        """
        Description: parse the dictionary file
        
        Parameters:
        - debug: Boolean - print debugging info to stdout
        
        Returns: a dictionary with all the columns in the data dictionary
        - structure: { *column: *{key: value} }
        - example: {'I94MODE': {'1': 'Air', '2': 'Sea', '3': 'Land', '9': 'Not reported'}}
        """
        self.debug = debug

        try:
            self.open_file()
            self.parse_file()
        except EOFError:
            self.log('EOF')
        except Exception as ex:
            print(f"DataDictionaryParser caught exception:")
            print(traceback.format_exc())
        finally:
            self.close_file()
            
        return self.data.get_data()
            
    def parse_file(self):
        self.next_line()
        self.parse_header()
        self.parse_body()
        
    def parse_header(self):
        self.log('>parse_header')
        while self.is_header_line():
            self.next_line()
        self.log('<parse_header')

    def parse_body(self):
        self.log('>parse_body')
        while True:
            self.parse_section()
        self.log('<parse_body')
            
    def is_header_line(self):
        self.log('is_header_line')
        
        return re.findall(DataDictionaryParser.header_line_regex, self.line)
            
    def parse_section(self):
        self.log('>parse_section')
        self.parse_empty_lines()
        self.parse_section_body()
        self.log('<parse_section')
        
    def parse_section_body(self):
        self.log('>parse_section_body')
        self.parse_section_content()
        self.parse_empty_lines()
        self.log('<parse_section_body')

    def parse_section_content(self):
        self.log('>parse_section_content')
        self.parse_comment()
        self.parse_maybe_data()
        self.log('<parse_section_content')
        
    def parse_comment(self):
        self.log('>parse_comment')
        
        self.column = self.parse_comment_start()
        
        self.parse_rest_of_comment()
        self.log('<parse_comment')
        
    def parse_comment_start(self):
        self.log('>parse_comment_start')
        
        column = re.findall(DataDictionaryParser.comment_start_regex, self.line)[0]
        
        self.log(f"\tcolumn: {column}")
        self.log('<parse_comment_start')
        return column
        
    def parse_rest_of_comment(self):
        self.log('>parse_rest_of_comment')
        self.parse_not_comment_end()
        self.parse_comment_end()
        self.log('<parse_rest_of_comment')
        
    def parse_not_comment_end(self):
        self.log('>parse_not_comment_end')
        
        while(self.parse_not_comment_end_line()):
            self.next_line()
            
        self.log('<parse_not_comment_end')
        
    def parse_not_comment_end_line(self):
        self.log('>parse_not_comment_end_line')
        self.log('<parse_not_comment_end_line')
        return not self.is_comment_end_line()
    
    def parse_comment_end(self):
        self.log('>parse_comment_end')
        
        if (self.is_comment_end_line()):
            self.next_line()
            
        self.log('<parse_comment_end')
            
    def parse_maybe_data(self):
        self.log('>parse_maybe_data')
        
        if self.is_empty_line():
            self.next_line()
        else:
            self.parse_value_data()
            
        self.log('<parse_maybe_data')
        
    def parse_value_data(self):
        self.log('>parse_value_data')
        self.parse_value()
        self.parse_data()
        self.log('<parse_value_data')        
        
    def parse_value(self):
        self.log('>parse_value')
        self.next_line()
        self.log('<parse_value')
        
    def parse_data(self):
        self.log('>parse_data')
        
        self.data.on_column(self.column)
        while self.parse_data_line():
            self.data.on_pair(self.key, self.value)
            
        self.log('<parse_data')
        
    def parse_data_line(self):
        self.log('>parse_data_line')
        
        data = re.findall(DataDictionaryParser.data_line_regexp, self.line)
        
        if data:
            self.key, self.value = data[0]
            self.log(f"{self.key} = {self.value}")
            self.next_line()
            
        self.log('<parse_data_line')
        
        return data
        
    def parse_empty_lines(self):
        self.log('>parse_empty_lines')
        
        while self.is_empty_line():
            self.next_line()
            
        self.log('<parse_empty_lines')
            
    def is_comment_end_line(self):
        matches = re.findall(DataDictionaryParser.comment_end_regex, self.line)
        return len(matches)
    
    def is_empty_line(self):
        self.log('is_empty_line')
        return re.findall(DataDictionaryParser.empty_line_regex, self.line)
    
    def next_line(self):
        self.line = self.file.readline()
        self.log(f"next_line: {self.line}")
        
        if len(self.line) == 0:
            raise EOFError
    
    def open_file(self):
        self.log('open_file')
        self.file = open(self.filepath, 'r')
    
    def close_file(self):
        self.log('close_file')
        self.file.close()
        
    def log(self, msg):
        if self.debug:
            print(msg)


