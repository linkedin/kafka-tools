from kafka.tools.assigner.actions import ActionModule


class ActionElect(ActionModule):
    name = "elect"
    helpstr = "Reelect partition leaders using preferred replica election"

    # This action is a no-op, so it doesn't need to override anything. It's just used to trigger a PLE
