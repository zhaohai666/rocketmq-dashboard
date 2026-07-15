/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import React, { useEffect, useState } from 'react';
import {
    Card,
    Table,
    Button,
    Space,
    Tag,
    Modal,
    Form,
    Input,
    InputNumber,
    notification,
    Spin,
    Row,
    Col,
    Statistic,
    Descriptions,
    Popconfirm,
    Tooltip,
    Alert,
} from 'antd';
import {
    PlusOutlined,
    ReloadOutlined,
    EditOutlined,
    DeleteOutlined,
    EyeOutlined,
    DatabaseOutlined,
    CheckCircleOutlined,
    CloseCircleOutlined,
    CloudServerOutlined,
} from '@ant-design/icons';
import { useLanguage } from '../../i18n/LanguageContext';
import { remoteApi } from '../../api/remoteApi/remoteApi';
import './Namespace.css';

const Namespace = () => {
    const { t } = useLanguage();
    const [form] = Form.useForm();

    const [loading, setLoading] = useState(false);
    const [namespaces, setNamespaces] = useState([]);
    const [namespaceSupported, setNamespaceSupported] = useState(true);
    const [detailModalVisible, setDetailModalVisible] = useState(false);
    const [formModalVisible, setFormModalVisible] = useState(false);
    const [selectedNamespace, setSelectedNamespace] = useState(null);
    const [isEdit, setIsEdit] = useState(false);
    const [stats, setStats] = useState({
        totalNamespaces: 0,
        enabledNamespaces: 0,
        totalTopicQuota: 0,
        totalGroupQuota: 0,
    });

    useEffect(() => {
        checkCapability();
        loadNamespaces();
    }, []);

    const checkCapability = () => {
        remoteApi.queryNamespaceCapability((resp) => {
            if (resp.status === 0) {
                setNamespaceSupported(resp.data?.namespaceSupported ?? false);
            }
        });
    };

    const loadNamespaces = () => {
        setLoading(true);
        remoteApi.queryNamespaceList((resp) => {
            setLoading(false);
            if (resp.status === 0) {
                const list = resp.data || [];
                setNamespaces(list);
                const enabledCount = list.filter(ns => ns.status === 'ENABLED').length;
                const totalTopicQuota = list.reduce((sum, ns) => sum + (ns.quotaConfig?.maxTopicCount || 0), 0);
                const totalGroupQuota = list.reduce((sum, ns) => sum + (ns.quotaConfig?.maxConsumerGroupCount || 0), 0);
                setStats({
                    totalNamespaces: list.length,
                    enabledNamespaces: enabledCount,
                    totalTopicQuota,
                    totalGroupQuota,
                });
            } else {
                notification.error({
                    message: resp.errMsg || t.NS_FETCH_FAILED || 'Failed to fetch namespaces',
                    duration: 2,
                });
            }
        });
    };

    const handleCreate = () => {
        setIsEdit(false);
        setSelectedNamespace(null);
        form.resetFields();
        setFormModalVisible(true);
    };

    const handleEdit = (record) => {
        setIsEdit(true);
        setSelectedNamespace(record);
        form.setFieldsValue({
            namespaceName: record.namespaceName,
            displayName: record.displayName,
            description: record.description,
            clusterName: record.clusterName,
            status: record.status,
            maxTopicCount: record.quotaConfig?.maxTopicCount,
            maxConsumerGroupCount: record.quotaConfig?.maxConsumerGroupCount,
            storageQuotaGB: record.quotaConfig?.storageQuotaGB,
            qpsLimit: record.quotaConfig?.qpsLimit,
            connectionLimit: record.quotaConfig?.connectionLimit,
        });
        setFormModalVisible(true);
    };

    const handleView = (record) => {
        setSelectedNamespace(record);
        setDetailModalVisible(true);
    };

    const handleDelete = (name) => {
        setLoading(true);
        remoteApi.deleteNamespace(name, (resp) => {
            setLoading(false);
            if (resp.status === 0) {
                notification.success({
                    message: t.NS_DELETE_SUCCESS || 'Namespace deleted successfully',
                    duration: 2,
                });
                loadNamespaces();
            } else {
                notification.error({
                    message: resp.errMsg || t.NS_DELETE_FAILED || 'Failed to delete namespace',
                    duration: 2,
                });
            }
        });
    };

    const handleSubmit = () => {
        form.validateFields().then((values) => {
            setLoading(true);
            const payload = {
                namespaceName: values.namespaceName,
                displayName: values.displayName,
                description: values.description,
                clusterName: values.clusterName,
                status: values.status || 'ENABLED',
                quotaConfig: {
                    maxTopicCount: values.maxTopicCount,
                    maxConsumerGroupCount: values.maxConsumerGroupCount,
                    storageQuotaGB: values.storageQuotaGB,
                    qpsLimit: values.qpsLimit,
                    connectionLimit: values.connectionLimit,
                },
            };

            const apiCall = isEdit
                ? remoteApi.updateNamespace
                : remoteApi.createNamespace;

            apiCall(payload, (resp) => {
                setLoading(false);
                if (resp.status === 0) {
                    notification.success({
                        message: isEdit
                            ? (t.NS_UPDATE_SUCCESS || 'Namespace updated successfully')
                            : (t.NS_CREATE_SUCCESS || 'Namespace created successfully'),
                        duration: 2,
                    });
                    setFormModalVisible(false);
                    form.resetFields();
                    loadNamespaces();
                } else {
                    notification.error({
                        message: resp.errMsg || (isEdit ? t.NS_UPDATE_FAILED : t.NS_CREATE_FAILED) || 'Operation failed',
                        duration: 2,
                    });
                }
            });
        });
    };

    const columns = [
        {
            title: t.NS_NAME || 'Namespace',
            dataIndex: 'namespaceName',
            key: 'namespaceName',
            render: (text, record) => (
                <Space>
                    <span style={{ fontWeight: 500 }}>{text}</span>
                    {record.defaultNamespace && (
                        <Tag color="blue">{t.NS_DEFAULT || 'Default'}</Tag>
                    )}
                </Space>
            ),
        },
        {
            title: t.NS_DISPLAY_NAME || 'Display Name',
            dataIndex: 'displayName',
            key: 'displayName',
        },
        {
            title: t.NS_CLUSTER || 'Cluster',
            dataIndex: 'clusterName',
            key: 'clusterName',
        },
        {
            title: t.STATUS || 'Status',
            dataIndex: 'status',
            key: 'status',
            render: (status) => (
                <Tag
                    color={status === 'ENABLED' ? 'success' : 'default'}
                    icon={status === 'ENABLED' ? <CheckCircleOutlined /> : <CloseCircleOutlined />}
                >
                    {status || 'UNKNOWN'}
                </Tag>
            ),
        },
        {
            title: t.NS_TOPIC_LIMIT || 'Topic Limit',
            key: 'topicLimit',
            render: (_, record) => record.quotaConfig?.maxTopicCount || '-',
        },
        {
            title: t.NS_GROUP_LIMIT || 'Group Limit',
            key: 'groupLimit',
            render: (_, record) => record.quotaConfig?.maxConsumerGroupCount || '-',
        },
        {
            title: t.NS_STORAGE || 'Storage (GB)',
            key: 'storage',
            render: (_, record) => record.quotaConfig?.storageQuotaGB || '-',
        },
        {
            title: t.OPERATION || 'Operation',
            key: 'operation',
            render: (_, record) => (
                <Space size="small">
                    <Tooltip title={t.NS_VIEW_DETAIL || 'View Detail'}>
                        <Button
                            type="link"
                            size="small"
                            icon={<EyeOutlined />}
                            onClick={() => handleView(record)}
                        />
                    </Tooltip>
                    {!record.defaultNamespace && (
                        <>
                            <Tooltip title={t.EDIT || 'Edit'}>
                                <Button
                                    type="link"
                                    size="small"
                                    icon={<EditOutlined />}
                                    onClick={() => handleEdit(record)}
                                />
                            </Tooltip>
                            <Popconfirm
                                title={t.NS_CONFIRM_DELETE || 'Are you sure to delete this namespace?'}
                                onConfirm={() => handleDelete(record.namespaceName)}
                                okText={t.YES || 'Yes'}
                                cancelText={t.NO || 'No'}
                            >
                                <Tooltip title={t.DELETE || 'Delete'}>
                                    <Button type="link" size="small" danger icon={<DeleteOutlined />} />
                                </Tooltip>
                            </Popconfirm>
                        </>
                    )}
                </Space>
            ),
        },
    ];

    if (!namespaceSupported) {
        return (
            <div className="namespace-container">
                <Alert
                    message={t.NS_NOT_SUPPORTED || 'Namespace Not Supported'}
                    description={t.NS_NOT_SUPPORTED_DESC || 'The current cluster architecture does not support namespace management. Please switch to a RocketMQ 5.0 Proxy cluster.'}
                    type="warning"
                    showIcon
                    icon={<CloudServerOutlined />}
                />
            </div>
        );
    }

    return (
        <div className="namespace-container">
            <Spin spinning={loading} tip={t.LOADING}>
                {/* Stats Cards */}
                <Row gutter={[16, 16]} style={{ marginBottom: 24 }}>
                    <Col xs={24} sm={12} md={6}>
                        <Card>
                            <Statistic
                                title={t.NS_TOTAL || 'Total Namespaces'}
                                value={stats.totalNamespaces}
                                prefix={<DatabaseOutlined />}
                                valueStyle={{ color: '#1890ff' }}
                            />
                        </Card>
                    </Col>
                    <Col xs={24} sm={12} md={6}>
                        <Card>
                            <Statistic
                                title={t.NS_ENABLED || 'Enabled'}
                                value={stats.enabledNamespaces}
                                suffix={`/ ${stats.totalNamespaces}`}
                                valueStyle={{ color: '#3f8600' }}
                            />
                        </Card>
                    </Col>
                    <Col xs={24} sm={12} md={6}>
                        <Card>
                            <Statistic
                                title={t.NS_TOTAL_TOPIC_QUOTA || 'Total Topic Quota'}
                                value={stats.totalTopicQuota}
                                valueStyle={{ color: '#1890ff' }}
                            />
                        </Card>
                    </Col>
                    <Col xs={24} sm={12} md={6}>
                        <Card>
                            <Statistic
                                title={t.NS_TOTAL_GROUP_QUOTA || 'Total Group Quota'}
                                value={stats.totalGroupQuota}
                                valueStyle={{ color: '#1890ff' }}
                            />
                        </Card>
                    </Col>
                </Row>

                {/* Action Bar */}
                <Card style={{ marginBottom: 16 }}>
                    <Space>
                        <Button type="primary" icon={<ReloadOutlined />} onClick={loadNamespaces}>
                            {t.REFRESH || 'Refresh'}
                        </Button>
                        <Button type="primary" icon={<PlusOutlined />} onClick={handleCreate}>
                            {t.NS_CREATE || 'Create Namespace'}
                        </Button>
                    </Space>
                </Card>

                {/* Namespace Table */}
                <Card title={t.NS_LIST || 'Namespace List'}>
                    <Table
                        columns={columns}
                        dataSource={namespaces}
                        rowKey="namespaceName"
                        pagination={false}
                        size="middle"
                    />
                </Card>
            </Spin>

            {/* Detail Modal */}
            <Modal
                title={`${t.NS_DETAIL || 'Namespace Detail'} - ${selectedNamespace?.namespaceName}`}
                open={detailModalVisible}
                onCancel={() => setDetailModalVisible(false)}
                footer={[
                    <Button key="close" onClick={() => setDetailModalVisible(false)}>
                        {t.CLOSE || 'Close'}
                    </Button>,
                ]}
                width={700}
            >
                {selectedNamespace && (
                    <Descriptions bordered column={2} size="small">
                        <Descriptions.Item label={t.NS_NAME || 'Namespace'} span={2}>
                            {selectedNamespace.namespaceName}
                        </Descriptions.Item>
                        <Descriptions.Item label={t.NS_DISPLAY_NAME || 'Display Name'}>
                            {selectedNamespace.displayName || '-'}
                        </Descriptions.Item>
                        <Descriptions.Item label={t.STATUS || 'Status'}>
                            <Tag color={selectedNamespace.status === 'ENABLED' ? 'success' : 'default'}>
                                {selectedNamespace.status}
                            </Tag>
                        </Descriptions.Item>
                        <Descriptions.Item label={t.NS_CLUSTER || 'Cluster'} span={2}>
                            {selectedNamespace.clusterName || '-'}
                        </Descriptions.Item>
                        <Descriptions.Item label={t.NS_DESCRIPTION || 'Description'} span={2}>
                            {selectedNamespace.description || '-'}
                        </Descriptions.Item>
                        <Descriptions.Item label={t.NS_DEFAULT || 'Default'}>
                            {selectedNamespace.defaultNamespace ? t.YES || 'Yes' : t.NO || 'No'}
                        </Descriptions.Item>
                        <Descriptions.Item label={t.NS_CREATE_TIME || 'Create Time'}>
                            {selectedNamespace.createTime ? new Date(selectedNamespace.createTime).toLocaleString() : '-'}
                        </Descriptions.Item>

                        {selectedNamespace.quotaConfig && (
                            <>
                                <Descriptions.Item label={t.NS_QUOTA_SECTION || 'Quota Config'} span={2}>
                                    <strong>{t.NS_QUOTA_SECTION || 'Quota Configuration'}</strong>
                                </Descriptions.Item>
                                <Descriptions.Item label={t.NS_TOPIC_LIMIT || 'Max Topics'}>
                                    {selectedNamespace.quotaConfig.maxTopicCount || '-'}
                                </Descriptions.Item>
                                <Descriptions.Item label={t.NS_GROUP_LIMIT || 'Max Groups'}>
                                    {selectedNamespace.quotaConfig.maxConsumerGroupCount || '-'}
                                </Descriptions.Item>
                                <Descriptions.Item label={t.NS_STORAGE || 'Storage (GB)'}>
                                    {selectedNamespace.quotaConfig.storageQuotaGB || '-'}
                                </Descriptions.Item>
                                <Descriptions.Item label={t.NS_QPS_LIMIT || 'QPS Limit'}>
                                    {selectedNamespace.quotaConfig.qpsLimit || '-'}
                                </Descriptions.Item>
                                <Descriptions.Item label={t.NS_CONN_LIMIT || 'Connection Limit'}>
                                    {selectedNamespace.quotaConfig.connectionLimit || '-'}
                                </Descriptions.Item>
                            </>
                        )}
                    </Descriptions>
                )}
            </Modal>

            {/* Create/Edit Modal */}
            <Modal
                title={isEdit ? (t.NS_EDIT || 'Edit Namespace') : (t.NS_CREATE || 'Create Namespace')}
                open={formModalVisible}
                onCancel={() => {
                    setFormModalVisible(false);
                    form.resetFields();
                }}
                onOk={handleSubmit}
                okText={isEdit ? (t.UPDATE || 'Update') : (t.ADD || 'Create')}
                cancelText={t.CANCEL || 'Cancel'}
                width={600}
            >
                <Form form={form} layout="vertical">
                    <Form.Item
                        name="namespaceName"
                        label={t.NS_NAME || 'Namespace Name'}
                        rules={[
                            { required: true, message: t.NS_NAME_REQUIRED || 'Please input namespace name' },
                            { pattern: /^[a-zA-Z0-9_-]+$/, message: t.NS_NAME_PATTERN || 'Only letters, numbers, hyphens and underscores' },
                        ]}
                    >
                        <Input
                            placeholder={t.NS_NAME_PLACEHOLDER || 'e.g., production-ns'}
                            disabled={isEdit}
                        />
                    </Form.Item>

                    <Row gutter={16}>
                        <Col span={12}>
                            <Form.Item name="displayName" label={t.NS_DISPLAY_NAME || 'Display Name'}>
                                <Input placeholder={t.NS_DISPLAY_NAME_PLACEHOLDER || 'e.g., Production'} />
                            </Form.Item>
                        </Col>
                        <Col span={12}>
                            <Form.Item name="clusterName" label={t.NS_CLUSTER || 'Cluster Name'}>
                                <Input placeholder={t.NS_CLUSTER_PLACEHOLDER || 'e.g., DefaultCluster'} />
                            </Form.Item>
                        </Col>
                    </Row>

                    <Form.Item name="description" label={t.NS_DESCRIPTION || 'Description'}>
                        <Input.TextArea rows={2} placeholder={t.NS_DESC_PLACEHOLDER || 'Describe this namespace...'} />
                    </Form.Item>

                    <Row gutter={16}>
                        <Col span={8}>
                            <Form.Item name="maxTopicCount" label={t.NS_TOPIC_LIMIT || 'Max Topics'}>
                                <InputNumber min={1} style={{ width: '100%' }} placeholder="1000" />
                            </Form.Item>
                        </Col>
                        <Col span={8}>
                            <Form.Item name="maxConsumerGroupCount" label={t.NS_GROUP_LIMIT || 'Max Groups'}>
                                <InputNumber min={1} style={{ width: '100%' }} placeholder="500" />
                            </Form.Item>
                        </Col>
                        <Col span={8}>
                            <Form.Item name="storageQuotaGB" label={t.NS_STORAGE || 'Storage (GB)'}>
                                <InputNumber min={1} style={{ width: '100%' }} placeholder="100" />
                            </Form.Item>
                        </Col>
                    </Row>

                    <Row gutter={16}>
                        <Col span={12}>
                            <Form.Item name="qpsLimit" label={t.NS_QPS_LIMIT || 'QPS Limit'}>
                                <InputNumber min={1} style={{ width: '100%' }} placeholder="10000" />
                            </Form.Item>
                        </Col>
                        <Col span={12}>
                            <Form.Item name="connectionLimit" label={t.NS_CONN_LIMIT || 'Connection Limit'}>
                                <InputNumber min={1} style={{ width: '100%' }} placeholder="5000" />
                            </Form.Item>
                        </Col>
                    </Row>
                </Form>
            </Modal>
        </div>
    );
};

export default Namespace;
