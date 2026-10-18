import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  docsSidebar: [
    {
      type: 'category',
      label: 'Getting Started',
      items: [
        'migrate-to-nestjs-outbox',
        'introduction',
        'quick-start',
      ],
    },
    'api-reference',
    'rabbitmq',
    'testing',
    'samples',
  ],
};

export default sidebars;
