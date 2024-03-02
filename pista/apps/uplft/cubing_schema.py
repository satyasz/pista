CUBING_REQ_SCHEMA = {
    "type": "object",
    "properties": {
        "companyData": {
            "type": "object",
            "properties": {
                "featureId": {"type": ["string", "null"]},
                "companyKey": {"type": ["string", "null"]}
            },
            "required": []
        },
        "containerData": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "containerType": {"type": "string"},
                    "containerSize": {"type": "string"},
                    "maxWeight": {"type": "number"},
                    "maxVolume": {"type": "number"},
                    "height": {"type": "number"},
                    "width": {"type": "number"},
                    "length": {"type": "number"},
                    "emptyContainerWeight": {"type": "number"},
                    "maxQuantity": {"type": "integer"},
                    "oversizeFlag": {"type": "boolean"}
                },
                "required": [
                    "id", "containerType", "containerSize", "maxWeight", "maxVolume", "height", "width",
                    "length", "emptyContainerWeight", "maxQuantity", "oversizeFlag"
                ]
            }
        },
        "constraintData": {
            "type": "object",
            "properties": {
                "isDimensionsCheckNeeded": {"type": "boolean"},
                "containerTypeHierarchyForBag": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                "containerTypeHierarchyForBox": {
                    "type": "array",
                    "items": {"type": "string"}
                }
            },
            "required": ["isDimensionsCheckNeeded"],
        },
        "orderData": {
            "type": "object",
            "properties": {
                "orderId": {"type": "string"},
                "orderDetails": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "orderQty": {"type": "integer"},
                            "itemId": {"type": ["string", "number"]},
                            "containerType": {"type": "string"},
                            "unitWeight": {"type": "number"},
                            "unitVolume": {"type": "number"},
                            "unitWidth": {"type": "number"},
                            "unitLength": {"type": "number"},
                            "unitHeight": {"type": "number"}
                        },
                        "required": [
                            "orderQty", "itemId", "containerType", "unitWeight", "unitVolume", "unitWidth",
                            "unitLength", "unitHeight"
                        ],
                    }
                }
            },
            "required": ["orderId", "orderDetails"],
        }
    },
    "required": ["containerData", "constraintData", "orderData"]
}

CUBING_REQ_SCHEMA_NEW = {
    "type": "object",
    "properties": {
        "companyData": {
            "type": "object",
            "properties": {
                "featureId": {"type": ["string", "null"]},
                "companyKey": {"type": ["string", "null"]}
            },
            "required": []
        },
        "containerData": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "containerType": {"type": "string"},
                    "containerSize": {"type": "string"},
                    "maxWeight": {"type": "number"},
                    "maxVolume": {"type": "number"},
                    "height": {"type": "number"},
                    "width": {"type": "number"},
                    "length": {"type": "number"},
                    "emptyContainerWeight": {"type": "number"},
                    "maxQuantity": {"type": "integer"},
                    "oversizeFlag": {"type": "boolean"}
                },
                "required": [
                    "id", "containerType", "containerSize", "maxWeight", "maxVolume", "height", "width",
                    "length", "emptyContainerWeight", "maxQuantity", "oversizeFlag"
                ]
            }
        },
        "constraintData": {
            "type": "object",
            "properties": {
                "isDimensionsCheckNeeded": {"type": "boolean"},
                "containerTypeHierarchyForBag": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                "containerTypeHierarchyForBox": {
                    "type": "array",
                    "items": {"type": "string"}
                }
            },
            "required": ["isDimensionsCheckNeeded"],
        },
        "orderData": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "orderId": {"type": ["string", "number"]},
                    "orderQty": {"type": "integer"},
                    "itemId": {"type": ["string", "number"]},
                    "containerType": {"type": "string"},
                    "unitWeight": {"type": "number"},
                    "unitVolume": {"type": "number"},
                    "unitWidth": {"type": "number"},
                    "unitLength": {"type": "number"},
                    "unitHeight": {"type": "number"},
                    "breakAttribute": {"type": ["string", "null"]},
                    "criticalDim1": {"type": ["number", "null"]},
                    "criticalDim2": {"type": ["number", "null"]},
                    "criticalDim3": {"type": ["number", "null"]}
                },
                "required": [
                    "orderId", "orderQty", "itemId", "containerType",
                    "unitWeight", "unitVolume", "unitWidth", "unitLength", "unitHeight"
                ]
            }
        },
    },
    "required": ["containerData", "constraintData", "orderData"]
}

CUBING_RESP_SCHEMA = {
    "type": "object",
    "properties": {
        "status": {"type": "string"},
        "timestamp": {"type": "string"},
        "message": {"type": "string"},
        "data": {
            "type": "object",
            "properties": {
                "response": {
                    "type": "object",
                    "properties": {
                        "cubingBatchId": {"type": "string"},
                        "orderId": {"type": "string"},
                        "totalItems": {"type": "string"},
                        "totalContainers": {"type": "string"},
                        "totalItemsWeight": {"type": "string"},
                        "totalTime": {"type": "string"},
                        "outputDetails": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "containerData": {
                                        "type": "object",
                                        "properties": {
                                            "id": {"type": "string"},
                                            "containerType": {"type": "string"},
                                            "containerSize": {"type": "string"},
                                            "maxWeight": {"type": "number"},
                                            "maxVolume": {"type": "number"},
                                            "length": {"type": "number"},
                                            "width": {"type": "number"},
                                            "height": {"type": "number"},
                                            "maxQuantity": {"type": "number"},
                                            "emptyContainerWeight": {"type": "number"},
                                            "oversizeFlag": {"type": "boolean"}
                                        },
                                        "required": [
                                            "id", "containerType", "containerSize",
                                            "maxWeight", "maxVolume", "length", "width", "height",
                                            "maxQuantity", "emptyContainerWeight", "oversizeFlag"
                                        ]
                                    },
                                    "outputData": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "itemId": {"type": "string"},
                                                "orderQty": {"type": "number"},
                                                "containerType": {"type": "string"},
                                                "unitWeight": {"type": "number"},
                                                "unitVolume": {"type": "number"},
                                                "unitWidth": {"type": "number"},
                                                "unitLength": {"type": "number"},
                                                "unitHeight": {"type": "number"},
                                                "breakAttribute": {},
                                                "criticalDim1": {},
                                                "criticalDim2": {},
                                                "criticalDim3": {},
                                                "orientationIndicator": {},
                                                "stackableFlag": {},
                                                "totalWeight": {"type": "number"},
                                                "totalVolume": {"type": "number"},
                                                "totalWidth": {"type": "number"},
                                                "totalHeight": {"type": "number"},
                                                "totalLength": {"type": "number"}
                                            },
                                            "required": [
                                                "itemId", "orderQty", "containerType",
                                                "unitWeight", "unitVolume", "unitWidth", "unitLength", "unitHeight",
                                                "breakAttribute",
                                                "criticalDim1", "criticalDim2", "criticalDim3",
                                                "orientationIndicator", "stackableFlag",
                                                "totalWeight", "totalVolume", "totalWidth", "totalHeight", "totalLength"
                                            ]
                                        }
                                    },
                                    "totalItems": {"type": "string"},
                                    "totalQuantity": {"type": "string"},
                                    "totalVolumeUsed": {"type": "string"},
                                    "totalVolumeUtilization": {"type": "string"},
                                    "totalWeightUsed": {"type": "string"},
                                    "totalWeightUtilization": {"type": "string"},
                                    "usedOversizedContainer": {"type": "string"}
                                },
                                "required": [
                                    "containerData", "outputData", "totalItems",
                                    "totalQuantity", "totalVolumeUsed", "totalVolumeUtilization", "totalWeightUsed",
                                    "totalWeightUtilization",
                                    "usedOversizedContainer"
                                ]
                            }
                        }
                    },
                    "required": [
                        "cubingBatchId", "orderId",
                        "totalItems", "totalContainers", "totalItemsWeight", "totalTime",
                        "outputDetails"
                    ]
                }
            },
            "required": ["response"]
        }
    },
    "required": ["status", "timestamp", "message", "data"]
}

CUBING_RESP_SCHEMA_NEW = {
    "type": "object",
    "properties": {
        "status": {"type": "string"},
        "timestamp": {"type": "string"},
        "message": {"type": "string"},
        "data": {
            "type": "object",
            "properties": {
                "response": {
                    "type": "object",
                    "properties": {
                        "cubingBatchId": {"type": "string"},
                        "orderId": {"type": "string"},
                        "totalItems": {"type": "string"},
                        "totalContainers": {"type": "string"},
                        "totalItemsWeight": {"type": "string"},
                        "totalTime": {"type": "string"},
                        "outputDetails": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "containerData": {
                                        "type": "object",
                                        "properties": {
                                            "id": {"type": "string"},
                                            "containerType": {"type": "string"},
                                            "containerSize": {"type": "string"},
                                            "maxWeight": {"type": "number"},
                                            "maxVolume": {"type": "number"},
                                            "length": {"type": "number"},
                                            "width": {"type": "number"},
                                            "height": {"type": "number"},
                                            "maxQuantity": {"type": "number"},
                                            "emptyContainerWeight": {"type": "number"},
                                            "oversizeFlag": {"type": "boolean"}
                                        },
                                        "required": [
                                            "id", "containerType", "containerSize",
                                            "maxWeight", "maxVolume", "length", "width", "height",
                                            "maxQuantity", "emptyContainerWeight", "oversizeFlag"
                                        ]
                                    },
                                    "outputData": {
                                        "anyOf": [
                                            {
                                                "type": "array",
                                                "items": {
                                                    "type": "object",
                                                    "properties": {
                                                        "itemId": {"type": "string"},
                                                        "orderQty": {"type": "number"},
                                                        "containerType": {"type": "string"},
                                                        "unitWeight": {"type": "number"},
                                                        "unitVolume": {"type": "number"},
                                                        "unitWidth": {"type": "number"},
                                                        "unitLength": {"type": "number"},
                                                        "unitHeight": {"type": "number"},
                                                        "breakAttribute": {},
                                                        "criticalDim1": {},
                                                        "criticalDim2": {},
                                                        "criticalDim3": {},
                                                        "orientationIndicator": {},
                                                        "stackableFlag": {},
                                                        "totalWeight": {"type": "number"},
                                                        "totalVolume": {"type": "number"},
                                                        "totalWidth": {"type": "number"},
                                                        "totalHeight": {"type": "number"},
                                                        "totalLength": {"type": "number"}
                                                    },
                                                    "required": [
                                                        "itemId", "orderQty", "containerType",
                                                        "unitWeight", "unitVolume", "unitWidth", "unitLength",
                                                        "unitHeight",
                                                        "breakAttribute",
                                                        "criticalDim1", "criticalDim2", "criticalDim3",
                                                        "orientationIndicator", "stackableFlag",
                                                        "totalWeight", "totalVolume", "totalWidth", "totalHeight",
                                                        "totalLength"
                                                    ]
                                                }
                                            },
                                            {
                                                "type": "object",
                                                "properties": {
                                                    "itemId": {"type": "string"},
                                                    "orderQty": {"type": "number"},
                                                    "containerType": {"type": "string"},
                                                    "unitWeight": {"type": "number"},
                                                    "unitVolume": {"type": "number"},
                                                    "unitWidth": {"type": "number"},
                                                    "unitLength": {"type": "number"},
                                                    "unitHeight": {"type": "number"},
                                                    "breakAttribute": {},
                                                    "criticalDim1": {},
                                                    "criticalDim2": {},
                                                    "criticalDim3": {},
                                                    "orientationIndicator": {},
                                                    "stackableFlag": {},
                                                    "totalWeight": {"type": "number"},
                                                    "totalVolume": {"type": "number"},
                                                    "totalWidth": {"type": "number"},
                                                    "totalHeight": {"type": "number"},
                                                    "totalLength": {"type": "number"}
                                                },
                                                "required": [
                                                    "itemId", "orderQty", "containerType",
                                                    "unitWeight", "unitVolume", "unitWidth", "unitLength", "unitHeight",
                                                    "breakAttribute",
                                                    "criticalDim1", "criticalDim2", "criticalDim3",
                                                    "orientationIndicator", "stackableFlag",
                                                    "totalWeight", "totalVolume", "totalWidth", "totalHeight",
                                                    "totalLength"
                                                ]
                                            }
                                        ]
                                    },
                                    "totalItems": {"type": "string"},
                                    "totalQuantity": {"type": "string"},
                                    "totalVolumeUsed": {"type": "string"},
                                    "totalVolumeUtilization": {"type": "string"},
                                    "totalWeightUsed": {"type": "string"},
                                    "totalWeightUtilization": {"type": "string"},
                                    "usedOversizedContainer": {"type": "string"}
                                },
                                "required": [
                                    "containerData", "outputData", "totalItems",
                                    "totalQuantity", "totalVolumeUsed", "totalVolumeUtilization", "totalWeightUsed",
                                    "totalWeightUtilization",
                                    "usedOversizedContainer"
                                ]
                            }
                        }
                    },
                    "required": [
                        "cubingBatchId", "orderId",
                        "totalItems", "totalContainers", "totalItemsWeight", "totalTime",
                        "outputDetails"
                    ]
                }
            },
            "required": ["response"]
        }
    },
    "required": ["status", "timestamp", "message", "data"]
}
