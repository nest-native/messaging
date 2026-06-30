import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  docsSidebar: [
    {
      type: 'category',
      label: 'Getting Started',
      items: [
        'introduction',
        'quick-start',
      ],
    },
    'api-reference',
    'testing',
    'samples',
  ],
};

export default sidebars;
