import React from 'react';
import { describe, expect, it, vi } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import MultiSelectDropdown, { MultiSelectDropdownProps } from '../components/MultiSelectDropdown';

const OPTIONS = ['React', 'Node.js', 'TypeScript', 'GraphQL'];

type HarnessProps = Omit<MultiSelectDropdownProps, 'selected' | 'onChange'> & {
  initialSelected?: string[];
  onSelectionChange?: (values: string[]) => void;
};

const MultiSelectHarness: React.FC<HarnessProps> = ({
  initialSelected = [],
  onSelectionChange,
  ...props
}) => {
  const [selected, setSelected] = React.useState<string[]>(initialSelected);
  const handleChange = React.useCallback(
    (values: string[]) => {
      setSelected(values);
      onSelectionChange?.(values);
    },
    [onSelectionChange]
  );
  return (
    <MultiSelectDropdown
      {...props}
      selected={selected}
      onChange={handleChange}
    />
  );
};

describe('MultiSelectDropdown', () => {
  const getTrigger = () => screen.getByRole('button', { name: /options|skills/i });
  const openDropdown = () => {
    fireEvent.click(getTrigger());
  };

  it('renders placeholder text when nothing is selected', () => {
    render(<MultiSelectDropdown options={OPTIONS} selected={[]} onChange={() => {}} />);
    expect(screen.getByRole('button', { name: /select options/i })).toBeInTheDocument();
  });

  it('opens the dropdown and lists options on trigger click', () => {
    render(<MultiSelectDropdown options={OPTIONS} selected={[]} onChange={() => {}} />);
    openDropdown();
    expect(screen.getByRole('listbox')).toBeInTheDocument();
    OPTIONS.forEach((option) => {
      expect(screen.getByRole('option', { name: option })).toBeInTheDocument();
    });
  });

  it('closes when clicking outside the menu', () => {
    render(<MultiSelectDropdown options={OPTIONS} selected={[]} onChange={() => {}} />);
    openDropdown();
    expect(screen.getByRole('listbox')).toBeInTheDocument();
    fireEvent.mouseDown(document.body);
    expect(screen.queryByRole('listbox')).not.toBeInTheDocument();
  });

  it('invokes onChange with the selected option', () => {
    const handleChange = vi.fn();
    render(<MultiSelectDropdown options={OPTIONS} selected={[]} onChange={handleChange} />);
    openDropdown();
    fireEvent.click(screen.getByRole('option', { name: /React/ }));
    expect(handleChange).toHaveBeenCalledWith(['React']);
  });

  it('filters options based on search input', () => {
    render(
      <MultiSelectDropdown
        options={OPTIONS}
        selected={[]}
        onChange={() => {}}
        searchable
      />
    );
    openDropdown();
    fireEvent.change(screen.getByPlaceholderText(/search skills/i), { target: { value: 'Graph' } });
    expect(screen.getByRole('option', { name: 'GraphQL' })).toBeInTheDocument();
    expect(screen.queryByRole('option', { name: 'React' })).not.toBeInTheDocument();
  });

  it('supports selecting and clearing multiple options', () => {
    const handleChange = vi.fn();
    render(
      <MultiSelectHarness
        options={OPTIONS}
        placeholder="Filter by skills…"
        searchable
        selectAllOption
        clearable
        onSelectionChange={handleChange}
      />
    );
    openDropdown();
    fireEvent.click(screen.getByRole('option', { name: 'React' }));
    expect(handleChange).toHaveBeenLastCalledWith(['React']);
    fireEvent.click(screen.getByRole('option', { name: 'Node.js' }));
    expect(handleChange).toHaveBeenLastCalledWith(['React', 'Node.js']);
    fireEvent.click(screen.getByText(/clear all/i));
    expect(handleChange).toHaveBeenLastCalledWith([]);
  });

  it('selects all filtered options when using Select all control', () => {
    const handleChange = vi.fn();
    render(
      <MultiSelectHarness
        options={OPTIONS}
        placeholder="Filter by skills…"
        searchable
        selectAllOption
        onSelectionChange={handleChange}
      />
    );
    openDropdown();
    fireEvent.click(screen.getByText(/select all/i));
    expect(handleChange).toHaveBeenLastCalledWith(OPTIONS);
  });

  it('supports keyboard navigation and selection', () => {
    const handleChange = vi.fn();
    render(
      <MultiSelectHarness
        options={OPTIONS}
        placeholder="Filter by skills…"
        searchable
        onSelectionChange={handleChange}
      />
    );
  const trigger = getTrigger();
    trigger.focus();
    fireEvent.keyDown(trigger, { key: 'Enter' });
    const listbox = screen.getByRole('listbox');
    fireEvent.keyDown(listbox, { key: 'Enter' });
    expect(handleChange).toHaveBeenLastCalledWith(['React']);
  });
});
