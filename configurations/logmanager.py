import os
import logging
import sys
import inspect

class LogManager:
    informed_about_kusto_state = False
    kusto_logger = None

    def __init__(self, module_name, kusto_enabled=False):
        self.module_name = module_name
        self.sysout_logger = self._init_sysout_logger()

        self.kusto_enabled = False
        self.warning("Kusto logging disabled by config")
        LogManager.informed_about_kusto_state = True

    @staticmethod
    def get_logger(custom_name="default", 
                   init_call=False, 
                   force_custom=False, 
                   kusto_enabled=False):
        
        caller_frame = inspect.stack()[2]
        caller_module = inspect.getmodule(caller_frame[0])
        if force_custom:
            module_name = custom_name
        else:
            if caller_module:
                module_name = caller_module.__name__
            else:
                module_name = custom_name

        if kusto_enabled:
            return LogManager(module_name=module_name, kusto_enabled=kusto_enabled)
        else:
            return LogManager(module_name=module_name, kusto_enabled=False).sysout_logger