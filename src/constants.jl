"""
    BOOTSTRAP_SERVERS::String

Kafka configuration key for the initial broker list (`"bootstrap.servers"`).
"""
const BOOTSTRAP_SERVERS = "bootstrap.servers"

"""
    CLIENT_ID::String

Kafka configuration key for the client identifier (`"client.id"`).
"""
const CLIENT_ID = "client.id"

"""
    GROUP_ID::String

Kafka configuration key for the consumer group identifier (`"group.id"`).
"""
const GROUP_ID = "group.id"

"""
    AUTO_OFFSET_RESET::String

Kafka configuration key that controls where to start when no offset is committed
(`"auto.offset.reset"`).
"""
const AUTO_OFFSET_RESET = "auto.offset.reset"

"""
    ENABLE_AUTO_COMMIT::String

Kafka configuration key that enables automatic offset commits (`"enable.auto.commit"`).
"""
const ENABLE_AUTO_COMMIT = "enable.auto.commit"

"""
    RD_KAFKA_OFFSET_INVALID::Int

Sentinel offset value used by librdkafka to indicate an invalid or unset offset.
"""
const RD_KAFKA_OFFSET_INVALID = -1001

"""
    DEFAULT_LOG_FORMAT::String

Default logging format string used by the native logger.
"""
const DEFAULT_LOG_FORMAT = "{timestamp} [{level}] {message}"
