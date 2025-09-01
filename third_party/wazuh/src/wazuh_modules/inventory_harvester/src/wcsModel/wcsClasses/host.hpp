/*
 * Wazuh inventory harvester
 * Copyright (C) 2015, Wazuh Inc.
 * January 14, 2025.
 *
 * This program is free software; you can redistribute it
 * and/or modify it under the terms of the GNU General Public
 * License (version 2) as published by the FSF - Free Software
 * Foundation.
 */

#ifndef _HOST_WCS_MODEL_HPP
#define _HOST_WCS_MODEL_HPP

#include "os.hpp"
#include "reflectiveJson.hpp"
#include <string_view>

struct Host final
{
    std::string_view architecture;
    std::string_view hostname;
    std::string_view ip;
    OS os;

    REFLECTABLE(MAKE_FIELD("architecture", &Host::architecture),
                MAKE_FIELD("hostname", &Host::hostname),
                MAKE_FIELD("ip", &Host::ip),
                MAKE_FIELD("os", &Host::os));
};

#endif // _HOST_WCS_MODEL_HPP
