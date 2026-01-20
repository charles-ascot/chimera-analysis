"""
Betfair Field Dictionary

Maps Betfair API field codes/abbreviations to human-readable names and descriptions.
This dictionary is used to present data in a human-digestible format.

Source: Betfair Exchange Stream API documentation
"""

# Top-level message fields
TOP_LEVEL_FIELDS = {
    "op": {
        "name": "Operation Type",
        "description": "Message operation type (mcm=Market Change, ocm=Order Change)",
        "category": "Message Metadata"
    },
    "pt": {
        "name": "Publish Time",
        "description": "Timestamp when message was published (milliseconds since epoch)",
        "category": "Message Metadata"
    },
    "clk": {
        "name": "Clock Token",
        "description": "Sequence token for message ordering and recovery",
        "category": "Message Metadata"
    },
    "ct": {
        "name": "Change Type",
        "description": "Type of change (SUB_IMAGE, RESUB_DELTA, HEARTBEAT)",
        "category": "Message Metadata"
    },
    "status": {
        "name": "Stream Status",
        "description": "Stream health (null=OK, 503=latency issues)",
        "category": "Message Metadata"
    },
    "mc": {
        "name": "Market Changes",
        "description": "Array of market change objects",
        "category": "Market Data"
    },
    "oc": {
        "name": "Order Changes",
        "description": "Array of order change objects",
        "category": "Order Data"
    },
    "id": {
        "name": "Identifier",
        "description": "Unique identifier (market ID, selection ID, or bet ID depending on context)",
        "category": "Identifier"
    },
    "img": {
        "name": "Image Flag",
        "description": "If true, this is a full snapshot (not delta)",
        "category": "Message Metadata"
    },
    "con": {
        "name": "Conflated",
        "description": "True if multiple updates were combined in this message",
        "category": "Message Metadata"
    },
}

# Market Change (mc[]) fields
MARKET_CHANGE_FIELDS = {
    "id": {
        "name": "Market ID",
        "description": "Unique Betfair market identifier (e.g., 1.123456789)",
        "category": "Market Identity"
    },
    "marketDefinition": {
        "name": "Market Definition",
        "description": "Full market metadata including event, venue, rules",
        "category": "Market Metadata"
    },
    "rc": {
        "name": "Runner Changes",
        "description": "Array of price/volume updates for selections",
        "category": "Price Data"
    },
    "tv": {
        "name": "Total Volume",
        "description": "Total amount matched on this market (GBP)",
        "category": "Volume"
    },
    "img": {
        "name": "Image Flag",
        "description": "True = full market snapshot, False/null = delta update",
        "category": "Message Control"
    },
    "con": {
        "name": "Conflated",
        "description": "Multiple updates combined in this message",
        "category": "Message Control"
    },
}

# Market Definition fields
MARKET_DEFINITION_FIELDS = {
    "marketId": {
        "name": "Market ID",
        "description": "Unique market identifier",
        "category": "Market Identity"
    },
    "eventId": {
        "name": "Event ID",
        "description": "Unique event identifier (groups related markets)",
        "category": "Event Identity"
    },
    "eventName": {
        "name": "Event Name",
        "description": "Human-readable event name",
        "category": "Event Identity"
    },
    "marketName": {
        "name": "Market Name",
        "description": "Human-readable market name",
        "category": "Market Identity"
    },
    "marketType": {
        "name": "Market Type",
        "description": "Type of market (WIN, PLACE, FORECAST, etc.)",
        "category": "Market Type"
    },
    "venue": {
        "name": "Venue",
        "description": "Racing venue / location",
        "category": "Event Location"
    },
    "countryCode": {
        "name": "Country Code",
        "description": "ISO country code (GB, IE, US, etc.)",
        "category": "Event Location"
    },
    "timezone": {
        "name": "Timezone",
        "description": "Event timezone",
        "category": "Event Timing"
    },
    "marketTime": {
        "name": "Market Start Time",
        "description": "Scheduled start time (ISO format)",
        "category": "Event Timing"
    },
    "openDate": {
        "name": "Open Date",
        "description": "When market opened for betting",
        "category": "Event Timing"
    },
    "suspendTime": {
        "name": "Suspend Time",
        "description": "When market was/will be suspended",
        "category": "Event Timing"
    },
    "settleTime": {
        "name": "Settlement Time",
        "description": "When market was settled",
        "category": "Event Timing"
    },
    "status": {
        "name": "Market Status",
        "description": "Current status (OPEN, SUSPENDED, CLOSED)",
        "category": "Market State"
    },
    "inPlay": {
        "name": "In-Play",
        "description": "True if market is currently in-play",
        "category": "Market State"
    },
    "complete": {
        "name": "Complete",
        "description": "True if market is complete/settled",
        "category": "Market State"
    },
    "bspMarket": {
        "name": "BSP Market",
        "description": "True if Betfair Starting Price available",
        "category": "Market Features"
    },
    "bspReconciled": {
        "name": "BSP Reconciled",
        "description": "True if BSP has been calculated",
        "category": "Market State"
    },
    "turnInPlayEnabled": {
        "name": "Turn In-Play",
        "description": "True if market will turn in-play",
        "category": "Market Features"
    },
    "persistenceEnabled": {
        "name": "Persistence Enabled",
        "description": "True if bet persistence is available",
        "category": "Market Features"
    },
    "crossMatching": {
        "name": "Cross Matching",
        "description": "True if cross-matching enabled",
        "category": "Market Features"
    },
    "runnersVoidable": {
        "name": "Runners Voidable",
        "description": "True if runners can be voided",
        "category": "Market Features"
    },
    "numberOfActiveRunners": {
        "name": "Active Runners",
        "description": "Number of active selections",
        "category": "Market Structure"
    },
    "numberOfWinners": {
        "name": "Number of Winners",
        "description": "How many winners this market has",
        "category": "Market Structure"
    },
    "bettingType": {
        "name": "Betting Type",
        "description": "Type of betting (ODDS, LINE, RANGE, ASIAN_HANDICAP)",
        "category": "Market Type"
    },
    "marketBaseRate": {
        "name": "Commission Rate",
        "description": "Base commission rate (%)",
        "category": "Market Economics"
    },
    "discountAllowed": {
        "name": "Discount Allowed",
        "description": "True if discount rate applies",
        "category": "Market Economics"
    },
    "betDelay": {
        "name": "Bet Delay",
        "description": "Seconds orders are held before matching (in-play)",
        "category": "Market Rules"
    },
    "regulators": {
        "name": "Regulators",
        "description": "Market regulators",
        "category": "Compliance"
    },
    "eachWayDivisor": {
        "name": "Each Way Divisor",
        "description": "Divisor for place part of each-way bets",
        "category": "Market Rules"
    },
    "runners": {
        "name": "Runners List",
        "description": "Array of runner definitions",
        "category": "Market Structure"
    },
    "version": {
        "name": "Version",
        "description": "Market definition version number",
        "category": "Market Metadata"
    },
    "lineMaxUnit": {
        "name": "Line Max Unit",
        "description": "Maximum value for line markets",
        "category": "Line Market"
    },
    "lineMinUnit": {
        "name": "Line Min Unit",
        "description": "Minimum value for line markets",
        "category": "Line Market"
    },
    "lineInterval": {
        "name": "Line Interval",
        "description": "Step interval for line markets",
        "category": "Line Market"
    },
    "priceLadderDefinition": {
        "name": "Price Ladder",
        "description": "Price ladder definition",
        "category": "Market Structure"
    },
    "keyLineDefinition": {
        "name": "Key Line Definition",
        "description": "Key line handicap definition",
        "category": "Line Market"
    },
    "raceType": {
        "name": "Race Type",
        "description": "Type of race (Flat, Hurdle, Chase, etc.)",
        "category": "Racing Metadata"
    },
}

# Runner Definition fields (inside marketDefinition.runners[])
RUNNER_DEFINITION_FIELDS = {
    "id": {
        "name": "Selection ID",
        "description": "Unique runner/selection identifier",
        "category": "Runner Identity"
    },
    "sortPriority": {
        "name": "Sort Priority",
        "description": "Display order on Betfair website",
        "category": "Display"
    },
    "name": {
        "name": "Runner Name",
        "description": "Name of horse/selection",
        "category": "Runner Identity"
    },
    "status": {
        "name": "Runner Status",
        "description": "Status (ACTIVE, WINNER, LOSER, REMOVED)",
        "category": "Runner State"
    },
    "hc": {
        "name": "Handicap",
        "description": "Handicap value (if applicable)",
        "category": "Runner Attributes"
    },
    "adjustmentFactor": {
        "name": "Adjustment Factor",
        "description": "Rule 4 deduction factor if removed",
        "category": "Market Rules"
    },
    "bsp": {
        "name": "BSP",
        "description": "Betfair Starting Price",
        "category": "Price"
    },
    "removalDate": {
        "name": "Removal Date",
        "description": "When runner was removed (if applicable)",
        "category": "Runner State"
    },
}

# Runner Change (rc[]) fields - PRICE DATA
RUNNER_CHANGE_FIELDS = {
    "id": {
        "name": "Selection ID",
        "description": "Unique runner/selection identifier",
        "category": "Runner Identity"
    },
    "ltp": {
        "name": "Last Traded Price",
        "description": "Most recent matched price",
        "category": "Price - Core"
    },
    "tv": {
        "name": "Traded Volume",
        "description": "Total volume matched on this runner (GBP)",
        "category": "Volume"
    },
    # Level-based ladders (best 3 prices)
    "batb": {
        "name": "Best Available To Back",
        "description": "Best back prices [level, price, size]",
        "category": "Order Book - Back"
    },
    "batl": {
        "name": "Best Available To Lay",
        "description": "Best lay prices [level, price, size]",
        "category": "Order Book - Lay"
    },
    "bdatb": {
        "name": "Best Display Available To Back",
        "description": "Best display back prices (virtual)",
        "category": "Order Book - Back (Virtual)"
    },
    "bdatl": {
        "name": "Best Display Available To Lay",
        "description": "Best display lay prices (virtual)",
        "category": "Order Book - Lay (Virtual)"
    },
    # Full depth ladders
    "atb": {
        "name": "Available To Back",
        "description": "Full depth back ladder [price, size]",
        "category": "Order Book - Full Depth"
    },
    "atl": {
        "name": "Available To Lay",
        "description": "Full depth lay ladder [price, size]",
        "category": "Order Book - Full Depth"
    },
    # Traded ladder
    "trd": {
        "name": "Traded Ladder",
        "description": "All trades at each price [price, size]",
        "category": "Trade History"
    },
    # Starting prices
    "spb": {
        "name": "SP Back",
        "description": "Starting Price back offers [price, size]",
        "category": "Starting Price"
    },
    "spl": {
        "name": "SP Lay",
        "description": "Starting Price lay offers [price, size]",
        "category": "Starting Price"
    },
    "spn": {
        "name": "SP Near Price",
        "description": "Betfair SP near projection",
        "category": "Starting Price"
    },
    "spf": {
        "name": "SP Far Price",
        "description": "Betfair SP far projection",
        "category": "Starting Price"
    },
    # Handicap
    "hc": {
        "name": "Handicap",
        "description": "Handicap value for Asian markets",
        "category": "Handicap"
    },
}

# Order Change (oc[]) fields
ORDER_CHANGE_FIELDS = {
    "id": {
        "name": "Account ID",
        "description": "Betfair account identifier",
        "category": "Account"
    },
    "orc": {
        "name": "Order Runner Changes",
        "description": "Changes to orders on specific runners",
        "category": "Order Data"
    },
    "closed": {
        "name": "Closed",
        "description": "True if orders are closed",
        "category": "Order State"
    },
    "fullImage": {
        "name": "Full Image",
        "description": "True if this is complete order state",
        "category": "Message Control"
    },
}

# Unmatched Order (uo[]) fields
UNMATCHED_ORDER_FIELDS = {
    "id": {
        "name": "Bet ID",
        "description": "Unique bet identifier",
        "category": "Bet Identity"
    },
    "p": {
        "name": "Price",
        "description": "Order price",
        "category": "Bet Details"
    },
    "s": {
        "name": "Size",
        "description": "Order size (GBP)",
        "category": "Bet Details"
    },
    "side": {
        "name": "Side",
        "description": "B=Back, L=Lay",
        "category": "Bet Details"
    },
    "status": {
        "name": "Status",
        "description": "Order status (E=Executable, EC=Execution Complete)",
        "category": "Bet State"
    },
    "pt": {
        "name": "Persistence Type",
        "description": "L=Lapse, P=Persist, MOC=Market On Close",
        "category": "Bet Details"
    },
    "ot": {
        "name": "Order Type",
        "description": "L=Limit, MOC=Market On Close, LOC=Limit On Close",
        "category": "Bet Details"
    },
    "pd": {
        "name": "Placed Date",
        "description": "When order was placed",
        "category": "Bet Timing"
    },
    "md": {
        "name": "Matched Date",
        "description": "When last matched",
        "category": "Bet Timing"
    },
    "cd": {
        "name": "Cancelled Date",
        "description": "When cancelled (if applicable)",
        "category": "Bet Timing"
    },
    "ld": {
        "name": "Lapsed Date",
        "description": "When lapsed (if applicable)",
        "category": "Bet Timing"
    },
    "avp": {
        "name": "Average Price Matched",
        "description": "Average matched price",
        "category": "Bet Details"
    },
    "sm": {
        "name": "Size Matched",
        "description": "Amount matched",
        "category": "Bet Details"
    },
    "sr": {
        "name": "Size Remaining",
        "description": "Amount unmatched",
        "category": "Bet Details"
    },
    "sl": {
        "name": "Size Lapsed",
        "description": "Amount lapsed",
        "category": "Bet Details"
    },
    "sc": {
        "name": "Size Cancelled",
        "description": "Amount cancelled",
        "category": "Bet Details"
    },
    "sv": {
        "name": "Size Voided",
        "description": "Amount voided",
        "category": "Bet Details"
    },
    "rac": {
        "name": "Regulator Auth Code",
        "description": "Regulatory authorization code",
        "category": "Compliance"
    },
    "rc": {
        "name": "Regulator Code",
        "description": "Regulatory code",
        "category": "Compliance"
    },
    "rfo": {
        "name": "Reference Order",
        "description": "Reference to original order",
        "category": "Bet Identity"
    },
    "rfs": {
        "name": "Reference Strategy",
        "description": "Strategy reference",
        "category": "Bet Identity"
    },
    "lsrc": {
        "name": "Lapse Status Reason Code",
        "description": "Why order was lapsed",
        "category": "Bet State"
    },
}

# Matched Bets (mb) / Matched Lays (ml) fields
MATCHED_FIELDS = {
    "mb": {
        "name": "Matched Backs",
        "description": "Matched back bets [price, size]",
        "category": "Matched Bets"
    },
    "ml": {
        "name": "Matched Lays",
        "description": "Matched lay bets [price, size]",
        "category": "Matched Bets"
    },
}


def get_field_info(field_name: str, context: str = None) -> dict:
    """
    Get human-readable information for a field.
    
    Args:
        field_name: The field code (e.g., 'ltp', 'batb')
        context: Optional context hint ('market', 'runner', 'order')
    
    Returns:
        Dictionary with name, description, category
    """
    # Build combined lookup in order of specificity
    lookups = []
    
    if context == 'runner_change' or context == 'rc':
        lookups.append(RUNNER_CHANGE_FIELDS)
    elif context == 'market_definition' or context == 'marketDefinition':
        lookups.append(MARKET_DEFINITION_FIELDS)
    elif context == 'runner_definition':
        lookups.append(RUNNER_DEFINITION_FIELDS)
    elif context == 'market_change' or context == 'mc':
        lookups.append(MARKET_CHANGE_FIELDS)
    elif context == 'order' or context == 'uo':
        lookups.append(UNMATCHED_ORDER_FIELDS)
    elif context == 'order_change' or context == 'oc':
        lookups.append(ORDER_CHANGE_FIELDS)
    
    # Always include general lookups as fallback
    lookups.extend([
        RUNNER_CHANGE_FIELDS,
        MARKET_DEFINITION_FIELDS,
        RUNNER_DEFINITION_FIELDS,
        MARKET_CHANGE_FIELDS,
        ORDER_CHANGE_FIELDS,
        UNMATCHED_ORDER_FIELDS,
        MATCHED_FIELDS,
        TOP_LEVEL_FIELDS,
    ])
    
    # Search through lookups
    for lookup in lookups:
        if field_name in lookup:
            return lookup[field_name]
    
    # Return default if not found
    return {
        "name": field_name.replace('_', ' ').title(),
        "description": f"Field: {field_name}",
        "category": "Unknown"
    }


def get_all_known_fields() -> dict:
    """Return all known Betfair fields organized by category."""
    all_fields = {}
    
    for name, fields in [
        ("Top Level", TOP_LEVEL_FIELDS),
        ("Market Change", MARKET_CHANGE_FIELDS),
        ("Market Definition", MARKET_DEFINITION_FIELDS),
        ("Runner Definition", RUNNER_DEFINITION_FIELDS),
        ("Runner Change (Prices)", RUNNER_CHANGE_FIELDS),
        ("Order Change", ORDER_CHANGE_FIELDS),
        ("Unmatched Orders", UNMATCHED_ORDER_FIELDS),
        ("Matched Bets", MATCHED_FIELDS),
    ]:
        all_fields[name] = fields
    
    return all_fields


# Field categories for grouping in UI
FIELD_CATEGORIES = {
    "Message Metadata": {
        "icon": "📨",
        "description": "Stream message control and timing",
        "color": "#6B7280"  # Gray
    },
    "Market Identity": {
        "icon": "🏷️",
        "description": "Market identification fields",
        "color": "#3B82F6"  # Blue
    },
    "Event Identity": {
        "icon": "📅",
        "description": "Event/race identification",
        "color": "#8B5CF6"  # Purple
    },
    "Event Location": {
        "icon": "📍",
        "description": "Venue and location data",
        "color": "#EC4899"  # Pink
    },
    "Event Timing": {
        "icon": "⏰",
        "description": "Timing and scheduling",
        "color": "#F59E0B"  # Amber
    },
    "Market State": {
        "icon": "🔄",
        "description": "Current market status",
        "color": "#10B981"  # Emerald
    },
    "Market Features": {
        "icon": "⚙️",
        "description": "Market configuration options",
        "color": "#6366F1"  # Indigo
    },
    "Market Structure": {
        "icon": "🏗️",
        "description": "Market structure and composition",
        "color": "#14B8A6"  # Teal
    },
    "Market Type": {
        "icon": "📊",
        "description": "Type of betting market",
        "color": "#F97316"  # Orange
    },
    "Market Economics": {
        "icon": "💰",
        "description": "Commission and financial terms",
        "color": "#EF4444"  # Red
    },
    "Market Rules": {
        "icon": "📋",
        "description": "Betting rules and conditions",
        "color": "#84CC16"  # Lime
    },
    "Runner Identity": {
        "icon": "🏇",
        "description": "Horse/selection identification",
        "color": "#06B6D4"  # Cyan
    },
    "Runner State": {
        "icon": "🎯",
        "description": "Runner status and results",
        "color": "#22C55E"  # Green
    },
    "Runner Attributes": {
        "icon": "📈",
        "description": "Runner characteristics",
        "color": "#A855F7"  # Violet
    },
    "Price - Core": {
        "icon": "💲",
        "description": "Core pricing data",
        "color": "#EF4444"  # Red
    },
    "Volume": {
        "icon": "📊",
        "description": "Trading volume data",
        "color": "#3B82F6"  # Blue
    },
    "Order Book - Back": {
        "icon": "📗",
        "description": "Available to back (buy)",
        "color": "#22C55E"  # Green
    },
    "Order Book - Lay": {
        "icon": "📕",
        "description": "Available to lay (sell)",
        "color": "#EF4444"  # Red
    },
    "Order Book - Full Depth": {
        "icon": "📚",
        "description": "Complete order book",
        "color": "#6366F1"  # Indigo
    },
    "Trade History": {
        "icon": "📜",
        "description": "Historical trades",
        "color": "#F59E0B"  # Amber
    },
    "Starting Price": {
        "icon": "🏁",
        "description": "BSP related data",
        "color": "#EC4899"  # Pink
    },
    "Bet Identity": {
        "icon": "🎫",
        "description": "Bet identification",
        "color": "#8B5CF6"  # Purple
    },
    "Bet Details": {
        "icon": "📝",
        "description": "Bet parameters",
        "color": "#14B8A6"  # Teal
    },
    "Bet State": {
        "icon": "⚡",
        "description": "Bet status",
        "color": "#F97316"  # Orange
    },
    "Bet Timing": {
        "icon": "🕐",
        "description": "Bet timestamps",
        "color": "#6B7280"  # Gray
    },
    "Compliance": {
        "icon": "🛡️",
        "description": "Regulatory fields",
        "color": "#64748B"  # Slate
    },
    "Unknown": {
        "icon": "❓",
        "description": "Undocumented fields",
        "color": "#9CA3AF"  # Gray
    },
}
