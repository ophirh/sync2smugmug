
# Full sync: upload / download as needed. This is also useful for restoring from backup. It will NOT delete photos on
# either side - simply make sure that both online and disk have the same photos
POLICY_SYNC = 1

# The online version will reflect the images on disk (images that don't appear on disk will be deleted online)
POLICY_DISK_RULES = 2


AVAILABLE_POLICIES = (POLICY_SYNC, POLICY_DISK_RULES, )