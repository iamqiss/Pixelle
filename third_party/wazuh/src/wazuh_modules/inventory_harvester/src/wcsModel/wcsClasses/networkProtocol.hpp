/*
 * Wazuh inventory harvester
 * Copyright (C) 2015, Wazuh Inc.
 * March 27, 2025.
 *
 * This program is free software; you can redistribute it
 * and/or modify it under the terms of the GNU General Public
 * License (version 2) as published by the FSF - Free Software
 * Foundation.
 */

#ifndef _NETWORK_PROTOCOL_WCS_MODEL_HPP
#define _NETWORK_PROTOCOL_WCS_MODEL_HPP

#include "reflectiveJson.hpp"
#include <string_view>

struct Network final
{
    bool dhcp;
    std::string_view gateway;
    int64_t metric;
    std::string_view type;

    REFLECTABLE(MAKE_FIELD("dhcp", &Network::dhcp),
                MAKE_FIELD("gateway", &Network::gateway),
                MAKE_FIELD("metric", &Network::metric),
                MAKE_FIELD("type", &Network::type));
};

#endif // _NETWORK_PROTOCOL_WCS_MODEL_HPP
