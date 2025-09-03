/*
 * Wazuh Inventory Harvester - Clear elements
 * Copyright (C) 2015, Wazuh Inc.
 * January 22, 2025.
 *
 * This program is free software; you can redistribute it
 * and/or modify it under the terms of the GNU General Public
 * License (version 2) as published by the FSF - Free Software
 * Foundation.
 */

#ifndef _CLEAR_ELEMENTS_HPP
#define _CLEAR_ELEMENTS_HPP

#include "../wcsModel/noData.hpp"
#include "chainOfResponsability.hpp"
#include "indexerConnector.hpp"
#include <map>
#include <memory>

template<typename TContext, typename TIndexerConnector = IndexerConnector>
class ClearElements final : public AbstractHandler<std::shared_ptr<TContext>>
{
    const std::map<typename TContext::AffectedComponentType, std::unique_ptr<TIndexerConnector>, std::less<>>&
        m_indexerConnectorInstances;

public:
    // LCOV_EXCL_START
    /**
     * @brief Class destructor.
     *
     */
    ~ClearElements() = default;

    explicit ClearElements(
        const std::map<typename TContext::AffectedComponentType, std::unique_ptr<TIndexerConnector>, std::less<>>&
            indexerConnectorInstances)
        : m_indexerConnectorInstances(indexerConnectorInstances)
    {
    }
    // LCOV_EXCL_STOP

    /**
     * @brief Handles request and passes control to the next step of the chain.
     *
     * @param data Scan context.
     * @return std::shared_ptr<ScanContext> Abstract handler.
     */
    std::shared_ptr<TContext> handleRequest(std::shared_ptr<TContext> data) override
    {
        NoDataHarvester deleteElements;
        deleteElements.operation = "DELETED_BY_QUERY";
        deleteElements.id = data->agentId();
        m_indexerConnectorInstances.at(data->affectedComponentType())->publish(serializeToJSON(deleteElements));
        return AbstractHandler<std::shared_ptr<TContext>>::handleRequest(std::move(data));
    }
};

#endif // _CLEAR_ELEMENTS_HPP
