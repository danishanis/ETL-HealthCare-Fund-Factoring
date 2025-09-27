import logging
import os
import sys
from pathlib import Path
import requests

from configurations.logmanager import LogManager

class Config:
    instance = None

    def __init__(self) -> None:
        Config.set_instance()

    @staticmethod
    def set_instance():
        if not Config.instance:
            Config.instance = Config._Config()

    class _Config():

        def __init__(self) -> None:
            module_path = Path(__file__).parent
            self.PROJECT_ROOT = module_path.parent
            self._logger = self.get_logger(init_call=True)

        def get_logger(self, 
                       custom_name="default", 
                       init_call=False, 
                       force_custom=False, 
                       kusto_enabled=False):
            
            return LogManager.get_logger(custom_name=custom_name, 
                                         init_call=init_call, 
                                         force_custom=force_custom, 
                                         kusto_enabled=kusto_enabled) 
        
Config()

def cfg() -> Config._Config:
    return Config.instance