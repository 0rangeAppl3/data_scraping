from importlib import import_module

class Config:
  module = None
  @classmethod
  def get (cls):
    cls.module = import_module(f'config.config')