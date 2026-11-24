package webhooks.adapters

/** Encodes a vendor and delivery ID as one storage key without separator collisions. */
fun archiveKey(vendor: String, id: String) = "${vendor.length}:$vendor:$id"
