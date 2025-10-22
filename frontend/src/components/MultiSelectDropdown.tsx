import React, {
  useCallback,
  useEffect,
  useId,
  useMemo,
  useRef,
  useState,
} from 'react';
import { normalizeToken } from '../utils/urlFilters';

/**
 * Controlled multi-select dropdown component for filtering collections.
 * Callers own the selected values and provide all available options.
 */
export interface MultiSelectDropdownProps {
  options: string[];
  selected: string[];
  onChange: (selected: string[]) => void;
  placeholder?: string;
  label?: string;
  maxHeight?: string;
  searchable?: boolean;
  clearable?: boolean;
  selectAllOption?: boolean;
}

const DEFAULT_MAX_HEIGHT = '260px';
const MAX_VISIBLE_SELECTION = 3;

const dedupeOptions = (options: string[]): string[] => {
  const seen = new Set<string>();
  const deduped: string[] = [];
  options.forEach((option) => {
    const normalized = normalizeToken(option);
    if (!normalized || seen.has(normalized)) {
      return;
    }
    seen.add(normalized);
    deduped.push(option);
  });
  return deduped;
};

const MultiSelectDropdown: React.FC<MultiSelectDropdownProps> = ({
  options,
  selected,
  onChange,
  placeholder = 'Select options…',
  label,
  maxHeight = DEFAULT_MAX_HEIGHT,
  searchable = true,
  clearable = true,
  selectAllOption = true,
}) => {
  const dropdownId = useId();
  const containerRef = useRef<HTMLDivElement | null>(null);
  const searchInputRef = useRef<HTMLInputElement | null>(null);
  const optionsRef = useRef<Array<HTMLButtonElement | null>>([]);
  const [open, setOpen] = useState(false);
  const [searchTerm, setSearchTerm] = useState('');
  const [activeIndex, setActiveIndex] = useState<number>(-1);

  const dedupedOptions = useMemo(() => dedupeOptions(options), [options]);
  const normalizedSelected = useMemo(
    () => new Set(selected.map((value) => normalizeToken(value)).filter(Boolean)),
    [selected]
  );

  const filteredOptions = useMemo(() => {
    if (!searchTerm.trim()) {
      return dedupedOptions;
    }
    const needle = normalizeToken(searchTerm);
    if (!needle) {
      return dedupedOptions;
    }
    return dedupedOptions.filter((option) => normalizeToken(option).includes(needle));
  }, [dedupedOptions, searchTerm]);

  const hasSelection = normalizedSelected.size > 0;
  const selectedPreview = useMemo(() => {
    if (!selected.length) {
      return placeholder;
    }
    const preview = selected.slice(0, MAX_VISIBLE_SELECTION).join(', ');
    if (selected.length <= MAX_VISIBLE_SELECTION) {
      return preview;
    }
    return `${preview} (+${selected.length - MAX_VISIBLE_SELECTION} more)`;
  }, [placeholder, selected]);

  const closeDropdown = useCallback(() => {
    setOpen(false);
  }, []);

  const toggleDropdown = useCallback(() => {
    setOpen((prev) => !prev);
  }, []);

  const updateSelection = useCallback(
    (nextValues: string[]) => {
      const deduped = dedupeOptions(nextValues);
      onChange(deduped);
    },
    [onChange]
  );

  const handleOptionToggle = useCallback(
    (option: string) => {
      const normalized = normalizeToken(option);
      const isSelected = normalizedSelected.has(normalized);
      if (isSelected) {
        updateSelection(selected.filter((item) => normalizeToken(item) !== normalized));
        return;
      }
      updateSelection([...selected, option]);
    },
    [normalizedSelected, selected, updateSelection]
  );

  const handleClearAll = useCallback(() => {
    if (!hasSelection) {
      return;
    }
    updateSelection([]);
  }, [hasSelection, updateSelection]);

  const handleSelectAll = useCallback(() => {
    if (!filteredOptions.length) {
      return;
    }
    const normalizedCurrent = new Set(selected.map((value) => normalizeToken(value)).filter(Boolean));
    const additions = filteredOptions.filter((option) => !normalizedCurrent.has(normalizeToken(option)));
    if (!additions.length) {
      return;
    }
    updateSelection([...selected, ...additions]);
  }, [filteredOptions, selected, updateSelection]);

  useEffect(() => {
    if (!open) {
      setSearchTerm('');
      setActiveIndex(-1);
      return;
    }
    setActiveIndex(0);
    const focusTimer = window.setTimeout(() => {
      if (searchable && searchInputRef.current) {
        searchInputRef.current.focus({ preventScroll: true });
      } else if (optionsRef.current[0]) {
        optionsRef.current[0]?.focus({ preventScroll: true });
      }
    }, 0);
    return () => window.clearTimeout(focusTimer);
  }, [open, searchable, filteredOptions.length]);

  useEffect(() => {
    if (!open) {
      return undefined;
    }
    const handleClickOutside = (event: MouseEvent) => {
      if (!containerRef.current) return;
      if (containerRef.current.contains(event.target as Node)) {
        return;
      }
      closeDropdown();
    };
    const handleEscape = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        closeDropdown();
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    document.addEventListener('keydown', handleEscape);
    return () => {
      document.removeEventListener('mousedown', handleClickOutside);
      document.removeEventListener('keydown', handleEscape);
    };
  }, [closeDropdown, open]);

  useEffect(() => {
    optionsRef.current = [];
  }, [filteredOptions]);

  const moveActiveIndex = useCallback(
    (direction: 1 | -1) => {
      if (!filteredOptions.length) {
        return;
      }
      setActiveIndex((prev) => {
        const next = prev + direction;
        if (next < 0) {
          return filteredOptions.length - 1;
        }
        if (next >= filteredOptions.length) {
          return 0;
        }
        return next;
      });
    },
    [filteredOptions]
  );

  const handleTriggerKeyDown = useCallback(
    (event: React.KeyboardEvent<HTMLButtonElement>) => {
      if (event.key === 'ArrowDown' || event.key === 'Enter' || event.key === ' ') {
        event.preventDefault();
        if (!open) {
          setOpen(true);
        } else {
          moveActiveIndex(1);
        }
      }
      if (event.key === 'ArrowUp' && open) {
        event.preventDefault();
        moveActiveIndex(-1);
      }
      if (event.key === 'Escape' && open) {
        event.preventDefault();
        closeDropdown();
      }
    },
    [closeDropdown, moveActiveIndex, open]
  );

  const handleMenuKeyDown = useCallback(
    (event: React.KeyboardEvent<HTMLDivElement>) => {
      if (event.key === 'ArrowDown') {
        event.preventDefault();
        moveActiveIndex(1);
        return;
      }
      if (event.key === 'ArrowUp') {
        event.preventDefault();
        moveActiveIndex(-1);
        return;
      }
      if (event.key === 'Home') {
        event.preventDefault();
        setActiveIndex(0);
        optionsRef.current[0]?.focus({ preventScroll: true });
        return;
      }
      if (event.key === 'End') {
        event.preventDefault();
        const lastIndex = filteredOptions.length - 1;
        setActiveIndex(lastIndex);
        optionsRef.current[lastIndex]?.focus({ preventScroll: true });
        return;
      }
      if (event.key === 'Enter' || event.key === ' ') {
        event.preventDefault();
        const option = filteredOptions[activeIndex];
        if (option) {
          handleOptionToggle(option);
        }
      }
      if (event.key === 'Escape') {
        event.preventDefault();
        closeDropdown();
      }
    },
    [activeIndex, closeDropdown, filteredOptions, handleOptionToggle, moveActiveIndex]
  );

  useEffect(() => {
    if (activeIndex < 0) {
      return;
    }
    const optionNode = optionsRef.current[activeIndex];
    optionNode?.focus({ preventScroll: true });
  }, [activeIndex]);

  return (
    <div ref={containerRef} style={{ position: 'relative' }}>
      {label && (
        <label htmlFor={dropdownId} style={{ display: 'block', fontSize: 13, fontWeight: 600, marginBottom: 8, color: '#94a3b8' }}>
          {label}
        </label>
      )}
      <button
        id={dropdownId}
        type="button"
        onClick={toggleDropdown}
        onKeyDown={handleTriggerKeyDown}
        aria-haspopup="listbox"
        aria-expanded={open}
        aria-controls={`${dropdownId}-menu`}
        style={{
          width: '100%',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          gap: 12,
          padding: '10px 14px',
          borderRadius: 12,
          border: open ? '1px solid rgba(16,185,129,0.5)' : '1px solid rgba(255,255,255,0.08)',
          background: '#0f172a',
          color: hasSelection ? '#e2e8f0' : '#64748b',
          cursor: 'pointer',
          transition: 'border 0.2s ease, box-shadow 0.2s ease',
          boxShadow: open ? '0 0 0 4px rgba(16,185,129,0.12)' : 'none',
        }}
      >
        <span style={{ flex: '1 1 auto', textAlign: 'left', fontSize: 13, lineHeight: 1.4 }}>
          {selectedPreview}
        </span>
        <span
          aria-hidden
          style={{
            display: 'inline-flex',
            alignItems: 'center',
            justifyContent: 'center',
            width: 24,
            height: 24,
            borderRadius: 999,
            background: 'rgba(255,255,255,0.08)',
            color: '#e2e8f0',
            fontSize: 12,
            fontWeight: 700,
            transform: open ? 'rotate(180deg)' : 'rotate(0deg)',
            transition: 'transform 0.2s ease',
          }}
        >
          ∨
        </span>
      </button>

      {open && (
        <div
          id={`${dropdownId}-menu`}
          role="listbox"
          aria-multiselectable
          tabIndex={-1}
          onKeyDown={handleMenuKeyDown}
          style={{
            position: 'absolute',
            top: 'calc(100% + 8px)',
            left: 0,
            right: 0,
            zIndex: 10,
            borderRadius: 14,
            border: '1px solid rgba(255,255,255,0.08)',
            background: '#111827',
            boxShadow: '0 24px 48px rgba(15,23,42,0.32)',
            padding: 12,
            display: 'flex',
            flexDirection: 'column',
            gap: 8,
          }}
        >
          {searchable && (
            <div>
              <input
                ref={searchInputRef}
                type="text"
                value={searchTerm}
                onChange={(event) => setSearchTerm(event.target.value)}
                placeholder="Search skills…"
                style={{
                  width: '100%',
                  padding: '8px 10px',
                  borderRadius: 10,
                  border: '1px solid rgba(255,255,255,0.08)',
                  background: '#0f172a',
                  color: '#e2e8f0',
                  fontSize: 13,
                }}
              />
            </div>
          )}

          <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
            {selectAllOption && filteredOptions.length > 0 && (
              <button
                type="button"
                onClick={handleSelectAll}
                style={{
                  padding: '6px 10px',
                  borderRadius: 999,
                  border: '1px solid rgba(16,185,129,0.4)',
                  background: 'rgba(16,185,129,0.12)',
                  color: '#34d399',
                  fontSize: 12,
                  fontWeight: 600,
                  cursor: 'pointer',
                }}
              >
                Select all ({filteredOptions.length})
              </button>
            )}
            {clearable && hasSelection && (
              <button
                type="button"
                onClick={handleClearAll}
                style={{
                  padding: '6px 10px',
                  borderRadius: 999,
                  border: '1px solid rgba(255,255,255,0.12)',
                  background: 'rgba(255,255,255,0.05)',
                  color: '#94a3b8',
                  fontSize: 12,
                  fontWeight: 600,
                  cursor: 'pointer',
                }}
              >
                Clear all
              </button>
            )}
          </div>

          <div
            style={{
              maxHeight,
              overflowY: 'auto',
              paddingRight: 4,
            }}
          >
            {filteredOptions.length === 0 ? (
              <div style={{ color: '#64748b', fontSize: 13, padding: '12px 4px' }}>No skills found</div>
            ) : (
              filteredOptions.map((option, index) => {
                const normalized = normalizeToken(option);
                const isSelected = normalizedSelected.has(normalized);
                const isActive = index === activeIndex;
                return (
                  <button
                    key={normalized + index}
                    ref={(node) => {
                      optionsRef.current[index] = node;
                    }}
                    type="button"
                    role="option"
                    aria-selected={isSelected}
                    onClick={() => handleOptionToggle(option)}
                    onMouseEnter={() => setActiveIndex(index)}
                    style={{
                      width: '100%',
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'space-between',
                      gap: 12,
                      padding: '10px 12px',
                      marginBottom: 4,
                      borderRadius: 10,
                      border: isActive ? '1px solid rgba(148,163,184,0.4)' : '1px solid transparent',
                      background: isSelected ? 'rgba(16,185,129,0.18)' : 'transparent',
                      color: isSelected ? '#d1fae5' : '#e2e8f0',
                      fontSize: 13,
                      fontWeight: isSelected ? 600 : 500,
                      cursor: 'pointer',
                      transition: 'background 0.2s ease, border 0.2s ease',
                    }}
                  >
                    <span style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                      <span
                        aria-hidden
                        style={{
                          width: 18,
                          height: 18,
                          borderRadius: 4,
                          border: isSelected ? '1px solid rgba(16,185,129,0.8)' : '1px solid rgba(148,163,184,0.5)',
                          background: isSelected ? 'rgba(16,185,129,0.4)' : 'transparent',
                          display: 'inline-flex',
                          alignItems: 'center',
                          justifyContent: 'center',
                          color: '#0f172a',
                          fontSize: 12,
                          fontWeight: 700,
                        }}
                      >
                        {isSelected ? '✓' : ''}
                      </span>
                      <span>{option}</span>
                    </span>
                    {isSelected && (
                      <span style={{ fontSize: 11, color: '#34d399', fontWeight: 600 }}>Selected</span>
                    )}
                  </button>
                );
              })
            )}
          </div>
        </div>
      )}
    </div>
  );
};

export default MultiSelectDropdown;
