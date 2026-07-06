import sys
import io
import re
import threading
import contextlib
import logging
import time

class RowLogFilter(io.TextIOBase):
    def __init__(self, output_buffer, desc, log_filename=None):
        self.buffer = ''
        self.output_buffer = output_buffer
        self.desc = desc
        self.current_row = None
        self.total_rows = None
        self.last_update = 0
        self.log_file = open(log_filename, 'w') if log_filename else None

    def write(self, s):
        self.buffer += s
        if '\n' in self.buffer:
            lines = self.buffer.split('\n')
            self.buffer = lines.pop()
            for line in lines:
                self.process_line(line)
        return len(s)

    def process_line(self, line):
        if line.startswith('__KEY__'):
            self.current_key = line[7:].strip()
            return
            
        import sys, time
        m_total = re.search(r'Market List has\s*(\d+)\s*Rows', line, re.IGNORECASE)
        if m_total:
            self.total_rows = int(m_total.group(1))

        m = re.search(r'Starting Allocation for row\s*:\s*(\d+)', line)
        if m:
            row_num = int(m.group(1))
            display_row = row_num + 1 if "connections" in self.desc.lower() else row_num
            self.current_row = display_row
            
            # Print simple progress bar to the original terminal
            total = self.total_rows if self.total_rows else 0
            if total > 0:
                percent = int((display_row / total) * 100)
                bar_len = 40
                filled = int(bar_len * display_row // total)
                bar = '█' * filled + '-' * (bar_len - filled)
                sys.__stdout__.write(f'\r{self.desc} (Row {display_row}/{total}) |{bar}| {percent}%')
            else:
                sys.__stdout__.write(f'\r{self.desc} (Row {display_row}/?)')
            sys.__stdout__.flush()
            
        prefix = f"[Row {self.current_row}] " if self.current_row is not None else ""
        
        # Check if the line already has the key (e.g. if it came from ownlogger)
        has_key = False
        if hasattr(self, 'current_key') and self.current_key:
            key_tag = f"[{self.current_key}]"
            if key_tag in line:
                has_key = True
                
        # Append the key if it wasn't already added
        if hasattr(self, 'current_key') and self.current_key and not has_key:
            line = f"{line} [{self.current_key}]"
            
        self.output_buffer.write(prefix + line + "\n")
        if self.log_file:
            self.log_file.write(prefix + line + "\n")
            self.log_file.flush()
            
    def close_log(self):
        import sys
        if self.buffer:
            self.process_line(self.buffer)
            self.buffer = ""
        if self.log_file:
            self.log_file.close()
        if self.total_rows is not None:
            sys.__stdout__.write(f'\r{self.desc} (Row {self.total_rows}/{self.total_rows}) |{"█"*40}| 100%\n')
        else:
            sys.__stdout__.write('\n')
        sys.__stdout__.flush()

def run_with_animation(desc, func, log_filename, *args, **kwargs):
    result = [None]
    output_captured = io.StringIO()
    
    row_filter = RowLogFilter(output_captured, desc, log_filename=log_filename)
    import sys, time, threading, contextlib
    sys.__stdout__.write(f"{desc} - initializing...\r")
    sys.__stdout__.flush()
    
    def worker():
        try:
            with contextlib.redirect_stdout(row_filter), contextlib.redirect_stderr(output_captured):
                try:
                    result[0] = func(*args, **kwargs)
                finally:
                    pass
        except Exception as e:
            result[0] = e
        finally:
            row_filter.close_log()

    t = threading.Thread(target=worker)
    t.start()
    
    while t.is_alive():
        if row_filter.total_rows is None:
            sys.__stdout__.write(f"{desc} - preparing data...\r")
            sys.__stdout__.flush()
        time.sleep(0.3)
        
    t.join()
    
    output_str = output_captured.getvalue()
            
    if isinstance(result[0], Exception):
        print(f"\n✖ {desc} failed with error: {result[0]}")
        lines = output_str.strip().split('\n')
        if lines:
            print("Last captured output:")
            for line in lines[-10:]:
                print(line)
        raise result[0]
        
    print(f"\n✔ {desc} completed successfully!")
    
    error_lines = [line for line in output_str.split('\n') if any(k in line for k in ['error', 'Error', 'exception', 'Exception'])]
    if error_lines:
        print("⚠ Captured Errors/Warnings:")
        for err in error_lines[:10]:
            print(f"  {err}")
            
    return result[0]
