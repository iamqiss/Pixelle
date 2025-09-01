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

#ifndef _PACKAGES_WCS_MODEL_HPP
#define _PACKAGES_WCS_MODEL_HPP

#include "reflectiveJson.hpp"
#include <string_view>

struct Package final
{
    std::string_view architecture;
    std::string_view description;
    std::string_view installed;
    std::string_view name;
    std::string_view path;
    std::int64_t size = DEFAULT_INT_VALUE;
    std::string_view type;
    std::string_view version;
    std::string_view vendor;
    struct Hotfix final
    {
        std::string_view name;

        REFLECTABLE(MAKE_FIELD("name", &Hotfix::name));
    };

    Hotfix hotfix;

    REFLECTABLE(MAKE_FIELD("architecture", &Package::architecture),
                MAKE_FIELD("description", &Package::description),
                MAKE_FIELD("installed", &Package::installed),
                MAKE_FIELD("name", &Package::name),
                MAKE_FIELD("path", &Package::path),
                MAKE_FIELD("size", &Package::size),
                MAKE_FIELD("type", &Package::type),
                MAKE_FIELD("version", &Package::version),
                MAKE_FIELD("vendor", &Package::vendor),
                MAKE_FIELD("hotfix", &Package::hotfix));
};

#endif // _PACKAGES_WCS_MODEL_HPP
