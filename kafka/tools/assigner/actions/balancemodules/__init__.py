from kafka.tools.assigner.actions import ActionModule


# Special class for balance modules to inherit from that skips configs
class ActionBalanceModule(ActionModule):
  @classmethod
  def configure_args(cls, subparser):
    pass
